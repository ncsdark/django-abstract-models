import time

from django.db.models import TextField, IntegerField, DateField

from common.models import (
    BaseModel,
    TimedModel,
    DeletableModel,
    AutoDeletableModel,
    UpdateLogModel,
    UpdatableModel,
    UpdatableLoggableModel,
    HistoryModel,
)
from common.exceptions import ProcessTerminatedError


class BaseModelImpl(BaseModel):
    int_field = IntegerField()
    char_field = TextField()


class TimedModelImpl(TimedModel):
    pass


class DeletableModelImpl(DeletableModel):
    pass


class AutoDeletableModelImpl(AutoDeletableModel):
    pass


class UpdatableModelImpl(UpdatableModel):
    @classmethod
    def _update(cls):
        for i in range(5):
            if not cls._must_terminate:
                time.sleep(0.1)
            else:
                raise ProcessTerminatedError


class UpdateLogModelImpl(UpdateLogModel):
    pass


class UpdatableLoggableModelImpl(UpdatableLoggableModel):
    log_model = UpdateLogModelImpl

    fail_update_param = 'fail_update_param'
    shorten_update_param = 'shorten_update_param'

    @classmethod
    def _update(cls, *args, **kwargs):
        if kwargs.get(cls.fail_update_param):
            raise RuntimeError
        iterations = 5 if not kwargs.get(cls.shorten_update_param) else 1
        for i in range(iterations):
            if not cls._must_terminate:
                time.sleep(1)
            else:
                raise ProcessTerminatedError


class HistoryModelImpl(HistoryModel):
    country = TextField()
    city = TextField()
    population = IntegerField()
    date_info = DateField()

    group_by = ['country', 'city']
    date_field = 'date_info'
