from threading import Lock
import traceback
from datetime import timedelta
from typing import Type

from django.db.models import Model, DateTimeField, TextField, BooleanField, F, Avg
from django.utils import timezone

from common.managers import HistoryManager, AutoDeleteManager
from common.exceptions import ProcessTerminatedError, OperationConflictsConfigError


class BaseModel(Model):
    class Meta:
        abstract = True

    def values(self, *fields, **expressions):
        return type(self).objects.filter(pk=self.pk).values(*fields, **expressions)[0]


class TimedModel(BaseModel):
    class Meta:
        abstract = True

    ordering: list[str] | tuple[str, ...] = ['time_created']

    time_created = DateTimeField(auto_now_add=True)

    @classmethod
    def get_ordering(cls, *args, **kwargs):
        return cls.ordering

    @classmethod
    def get_last_created_object(cls, *args, **kwargs):
        if not cls.objects.count():
            return
        return cls.objects.latest(*cls.get_ordering(*args, **kwargs))


class DeletableModel(TimedModel):
    class Meta:
        abstract = True

    max_objects_count: int = None

    @classmethod
    def get_max_objects_count(cls, *args, **kwargs):
        return cls.max_objects_count

    @classmethod
    def get_objects_to_delete(cls, *args, **kwargs):
        max_objects_count = cls.get_max_objects_count(*args, **kwargs)
        objects_to_delete_count = 0
        if max_objects_count:
            objects_to_delete_count = cls.objects.count() - max_objects_count
        if objects_to_delete_count <= 0:
            return cls.objects.none()
        return cls.objects.order_by(*cls.get_ordering(*args, **kwargs))[:objects_to_delete_count]

    @classmethod
    def try_delete_objects(cls, *args, **kwargs):
        objects_to_delete = cls.get_objects_to_delete(*args, **kwargs)
        return cls.objects.filter(pk__in=objects_to_delete.values_list('pk', flat=True)).delete()


class AutoDeletableModel(DeletableModel):
    class Meta:
        abstract = True

    objects = AutoDeleteManager()

    def save(self, force_insert=False, force_update=False, using=None, update_fields=None):
        super().save(force_insert, force_update, using, update_fields)
        self.try_delete_objects()


class LogModel(AutoDeletableModel):
    class Meta:
        abstract = True

    messages = TextField(null=True)


class ContinuousLogModel(LogModel):
    class Meta:
        abstract = True

    time_finished = DateTimeField(null=True)


class UpdateLogModel(ContinuousLogModel):
    class Meta:
        abstract = True

    was_canceled = BooleanField(default=False)
    was_terminated = BooleanField(default=False)
    has_failed = BooleanField(default=False)


class UpdatableModel(BaseModel):
    class Meta:
        abstract = True

    _update_lock = Lock()
    _check_lock = Lock()
    _time_started = None
    _must_terminate = False

    time_limit: timedelta = None

    @classmethod
    def get_time_limit(cls, *args, **kwargs):
        return cls.time_limit

    @classmethod
    def is_updating(cls, *args, **kwargs):
        return cls._update_lock.locked()

    @classmethod
    def is_can_start(cls, *args, **kwargs):
        if not cls.is_updating(*args, **kwargs):
            return True
        time_limit = cls.get_time_limit(*args, **kwargs)
        if not time_limit:
            return False
        if timezone.now() - cls._time_started > time_limit:
            return True
        return False

    @classmethod
    def _handle_cannot_start(cls, *args, **kwargs):
        pass

    @classmethod
    def _pre_update(cls, *args, **kwargs):
        pass

    @classmethod
    def _update(cls, *args, **kwargs):
        pass

    @classmethod
    def _post_update(cls, *args, **kwargs):
        pass

    @classmethod
    def _handle_exception(cls, exception, *args, **kwargs):
        pass

    @classmethod
    def update(cls, *args, **kwargs):
        cls._check_lock.acquire()

        if not cls.is_can_start(*args, **kwargs):
            cls._handle_cannot_start(*args, **kwargs)
            cls._check_lock.release()
            return False, None, None

        cls._must_terminate = True

        with cls._update_lock:
            cls._must_terminate = False
            cls._time_started = timezone.now()
            cls._check_lock.release()

            is_update_successful = True
            exception = None
            exception_after_handling_attempt = None

            try:
                cls._pre_update(*args, **kwargs)
                cls._update(*args, **kwargs)
                cls._post_update(*args, **kwargs)

            except Exception as exc:
                is_update_successful = False
                exception = exc
                try:
                    cls._handle_exception(exc, *args, **kwargs)
                except Exception as exc2:
                    exception_after_handling_attempt = exc2

            return is_update_successful, exception, exception_after_handling_attempt


class UpdatableLoggableModel(UpdatableModel):
    class Meta:
        abstract = True

    _log = None

    log_model: Type[UpdateLogModel] = None
    use_average_time: bool = False
    average_time_coefficient: float = 1

    @classmethod
    def get_log_model(cls, *args, **kwargs):
        log_model = cls.log_model
        if not issubclass(log_model, UpdateLogModel):
            raise OperationConflictsConfigError(
                f'Expected the log model (subclassed from {UpdateLogModel}) parameter to be set as a static field'
            )
        return log_model

    @classmethod
    def is_use_average_time(cls, *args, **kwargs):
        return cls.use_average_time

    @classmethod
    def get_average_time_coefficient(cls, *args, **kwargs):
        return cls.average_time_coefficient

    @classmethod
    def get_last_update_time(cls, filters, *args, **kwargs):
        log_model = cls.get_log_model(*args, **kwargs)
        objects = log_model.objects.filter(**filters)
        if not objects:
            return
        return objects.latest(log_model.time_finished.field.name).time_finished

    @classmethod
    def get_last_update_attempt_time(cls, *args, **kwargs):
        log_model = cls.get_log_model(*args, **kwargs)
        filters = {
            log_model.was_canceled.field.name: False,
        }
        return cls.get_last_update_time(filters, *args, **kwargs)

    @classmethod
    def get_last_successful_update_time(cls, *args, **kwargs):
        log_model = cls.get_log_model(*args, **kwargs)
        filters = {
            log_model.was_canceled.field.name: False,
            log_model.was_terminated.field.name: False,
            log_model.has_failed.field.name: False,
        }
        return cls.get_last_update_time(filters, *args, **kwargs)

    @classmethod
    def calc_average_update_time(cls, *args, **kwargs):
        log_model = cls.get_log_model(*args, **kwargs)
        return log_model \
            .objects \
            .filter(**{
                f'{log_model.time_created.field.name}__isnull': False,
                f'{log_model.time_finished.field.name}__isnull': False,
                log_model.was_canceled.field.name: False,
                log_model.was_terminated.field.name: False,
                log_model.has_failed.field.name: False,
            }) \
            .aggregate(avg=Avg(F(log_model.time_finished.field.name) - F(log_model.time_created.field.name))) \
            .get('avg')

    @classmethod
    def is_can_start(cls, *args, **kwargs):
        if not cls.is_updating(*args, **kwargs):
            return True

        time_limit = cls.get_time_limit(*args, **kwargs)
        use_average_time = cls.is_use_average_time(*args, **kwargs)
        average_time_coefficient = cls.get_average_time_coefficient(*args, **kwargs)
        average_time = None

        current_time = timezone.now() - cls._time_started

        if use_average_time:
            average_time = cls.calc_average_update_time(*args, **kwargs)
            if average_time and current_time > average_time * average_time_coefficient:
                return True

        if not average_time and time_limit and current_time > time_limit:
            return True

        return False

    @classmethod
    def _handle_cannot_start(cls, *args, **kwargs):
        log_model = cls.get_log_model(*args, **kwargs)
        log = log_model()
        log.time_finished = timezone.now()
        log.was_canceled = True
        log.save()

    @classmethod
    def _pre_update(cls, *args, **kwargs):
        log_model = cls.get_log_model(*args, **kwargs)
        cls._log = log_model.objects.create()

    @classmethod
    def _post_update(cls, *args, **kwargs):
        cls._log.time_finished = timezone.now()
        cls._log.save()

    @classmethod
    def _handle_exception(cls, exception, *args, **kwargs):
        if type(exception) is ProcessTerminatedError:
            cls._log.was_terminated = True
        else:
            cls._log.has_failed = True

        cls._log.messages = ''.join(traceback.TracebackException.from_exception(exception).format())
        cls._log.time_finished = timezone.now()
        cls._log.save()


class HistoryModel(TimedModel):
    class Meta:
        abstract = True

    objects = HistoryManager()

    group_by: tuple[str, ...] | list[str] | str = None
    date_field: str = None

    @classmethod
    def get_group_by(cls, *args, **kwargs):
        group_by = cls.group_by
        if not group_by:
            raise OperationConflictsConfigError(f'Expected the group by parameter to be set as a static field')
        return group_by

    @classmethod
    def get_date_field(cls, *args, **kwargs):
        if cls.date_field:
            return cls.date_field
        return cls.time_created.field.name

    @classmethod
    def get_objects_for_date(cls, date=None, *args, **kwargs):
        return cls.objects.for_date(
            cls.get_group_by(*args, **kwargs),
            cls.get_date_field(*args, **kwargs),
            date,
            *args,
            **kwargs
        )
