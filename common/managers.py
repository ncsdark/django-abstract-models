from django.db.models import QuerySet, Max, CharField, Value
from django.db.models.functions import Concat
from django.db.models.manager import BaseManager

from common.exceptions import OperationConflictsConfigError


class AutoDeleteQuerySet(QuerySet):
    def bulk_create(
            self,
            objs,
            batch_size=None,
            ignore_conflicts=False,
            update_conflicts=False,
            update_fields=None,
            unique_fields=None
    ):
        if self.model.objects.count() + len(objs) > self.model.get_max_objects_count():
            raise OperationConflictsConfigError('Cannot bulk create objs over max count (configured in the model)')
        res = super().bulk_create(objs, batch_size, ignore_conflicts, update_conflicts, update_fields, unique_fields)
        self.model.try_delete_objects()
        return res


class AutoDeleteManager(BaseManager.from_queryset(AutoDeleteQuerySet)):
    pass


class HistoryQuerySet(QuerySet):
    def for_date(self, group_by, date_field, date=None, *args, **kwargs):
        if not isinstance(group_by, (tuple, list)):
            group_by = [group_by]

        separator = Value('!@&%^$;#@$')
        concat_list = list(group_by)
        for i in range(1, 2 * len(group_by), 2):
            concat_list.insert(i, separator)

        filters = {f'{date_field}__lte': date} if date else {}

        result_oids = self \
            .filter(**filters) \
            .values(*group_by) \
            .alias(max_date=Max(date_field)) \
            .annotate(oid=Concat(*concat_list, 'max_date', output_field=CharField())) \
            .values_list('oid', flat=True)

        return self \
            .annotate(oid=Concat(*concat_list, date_field, output_field=CharField())) \
            .filter(oid__in=result_oids)


class HistoryManager(BaseManager.from_queryset(HistoryQuerySet)):
    pass
