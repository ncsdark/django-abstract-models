import time
from datetime import timedelta, datetime
from threading import Thread

from django.test import TestCase

from common.models import (
    DeletableModel,
    UpdatableModel,
    UpdatableLoggableModel,
)
from common.tests.models import (
    BaseModelImpl,
    TimedModelImpl,
    DeletableModelImpl,
    AutoDeletableModelImpl,
    UpdatableModelImpl,
    UpdatableLoggableModelImpl,
    HistoryModelImpl,
)
from common.exceptions import ProcessTerminatedError, OperationConflictsConfigError


class DefaultTestCase(TestCase):
    def assertQuerysetEqual(self, qs, values, transform=None, ordered=False, msg=None):
        super().assertQuerysetEqual(qs, values, transform, ordered, msg)


class BaseModelTestCase(DefaultTestCase):
    model = BaseModelImpl

    def test_values_on_single_object(self):
        char_field_name = self.model.char_field.field.name
        int_field_name = self.model.int_field.field.name
        char_value = 'test'
        int_value = 10
        values = {
            char_field_name: char_value,
            int_field_name: int_value,
        }
        obj = self.model.objects.create(**values)
        self.assertEqual(
            obj.values(),
            self.model.objects.values()[0]
        )
        self.assertEqual(
            obj.values(char_field_name, int_field_name),
            values
        )


class TimedModelTestCase(DefaultTestCase):
    model = TimedModelImpl

    def test_last_created_obj(self):
        mod = self.model
        self.assertEqual(mod.get_last_created_object(), None)
        o1 = mod.objects.create()
        self.assertEqual(mod.get_last_created_object(), o1)
        o2 = mod.objects.create()
        self.assertEqual(mod.get_last_created_object(), o2)
        o3 = mod.objects.create()
        self.assertEqual(mod.get_last_created_object(), o3)
        o4 = mod.objects.create()
        self.assertEqual(mod.get_last_created_object(), o4)
        o4.delete()
        o2.delete()
        self.assertEqual(mod.get_last_created_object(), o3)
        o1.delete()
        self.assertEqual(mod.get_last_created_object(), o3)
        o3.delete()
        self.assertEqual(mod.get_last_created_object(), None)


class DeletableModelTestCase(DefaultTestCase):
    model = DeletableModelImpl
    abstract_model = DeletableModel

    def setUp(self):
        self.model.max_objects_count = self.abstract_model.max_objects_count

    def test_objects_to_delete(self):
        mod = self.model
        self.assertQuerysetEqual(mod.get_objects_to_delete(), [])
        o1 = mod.objects.create()
        o2 = mod.objects.create()
        self.assertQuerysetEqual(mod.get_objects_to_delete(), [])
        mod.max_objects_count = 1
        self.assertQuerysetEqual(mod.get_objects_to_delete(), [o1])
        o3 = mod.objects.create()
        self.assertQuerysetEqual(mod.get_objects_to_delete(), [o1, o2])
        mod.max_objects_count = 3
        self.assertQuerysetEqual(mod.get_objects_to_delete(), [])
        o4 = mod.objects.create()
        self.assertQuerysetEqual(mod.get_objects_to_delete(), [o1])
        mod.max_objects_count = 1
        self.assertQuerysetEqual(mod.get_objects_to_delete(), [o1, o2, o3])
        o1.delete()
        self.assertQuerysetEqual(mod.get_objects_to_delete(), [o2, o3])
        o2.delete()
        o5 = mod.objects.create()
        self.assertQuerysetEqual(mod.get_objects_to_delete(), [o3, o4])

    def test_objects_to_delete_after_time_created_reversion(self):
        mod = self.model
        mod.max_objects_count = 2
        o1 = mod.objects.create()
        o2 = mod.objects.create()
        o3 = mod.objects.create()
        o4 = mod.objects.create()
        self.assertQuerysetEqual(mod.get_objects_to_delete(), [o1, o2])
        o1.time_created, o4.time_created = o4.time_created, o1.time_created
        o2.time_created, o3.time_created = o3.time_created, o2.time_created
        for x in [o1, o2, o3, o4]:
            x.save()
        self.assertQuerysetEqual(mod.get_objects_to_delete(), [o3, o4])

    def test_try_delete_objects(self):
        mod = self.model
        o1 = mod.objects.create()
        o2 = mod.objects.create()
        o3 = mod.objects.create()
        o4 = mod.objects.create()
        self.assertEqual(mod.try_delete_objects()[0], 0)
        self.assertQuerysetEqual(mod.objects.all(), [o1, o2, o3, o4])
        mod.max_objects_count = 2
        self.assertEqual(mod.try_delete_objects()[0], 2)
        self.assertQuerysetEqual(mod.objects.all(), [o3, o4])
        o3.delete()
        o4.delete()
        for x in [o1, o2, o3, o4]:
            x.save()
        mod.max_objects_count = 3
        self.assertEqual(mod.try_delete_objects()[0], 1)
        self.assertQuerysetEqual(mod.objects.all(), [o2, o3, o4])
        self.assertEqual(mod.try_delete_objects()[0], 0)
        self.assertQuerysetEqual(mod.objects.all(), [o2, o3, o4])


class AutoDeletableModelTestCase(DefaultTestCase):
    model = AutoDeletableModelImpl

    def test_auto_delete_after_create(self):
        mod = self.model
        mod.max_objects_count = 3
        o1 = mod.objects.create()
        self.assertQuerysetEqual(mod.objects.all(), [o1])
        o2 = mod.objects.create()
        self.assertQuerysetEqual(mod.objects.all(), [o1, o2])
        o3 = mod.objects.create()
        self.assertQuerysetEqual(mod.objects.all(), [o1, o2, o3])
        o4 = mod.objects.create()
        self.assertQuerysetEqual(mod.objects.all(), [o2, o3, o4])
        o5 = mod.objects.create()
        self.assertQuerysetEqual(mod.objects.all(), [o3, o4, o5])

    def test_auto_delete_after_bulk_create(self):
        mod = self.model
        mod.max_objects_count = 5
        self.assertRaises(OperationConflictsConfigError, mod.objects.bulk_create, [mod() for _ in range(6)])
        mod.objects.bulk_create([mod() for _ in range(3)])
        self.assertEqual(mod.objects.count(), 3)
        self.assertRaises(OperationConflictsConfigError, mod.objects.bulk_create, [mod() for _ in range(3)])
        mod.objects.bulk_create([mod() for _ in range(2)])
        self.assertEqual(mod.objects.count(), 5)


class UpdatableModelTestCase(DefaultTestCase):
    model = UpdatableModelImpl
    abstract_model = UpdatableModel

    def setUp(self):
        self.model._time_started = self.abstract_model._time_started
        self.model._must_terminate = self.abstract_model._must_terminate
        self.model.time_limit = self.abstract_model.time_limit

    def test_is_updating_and_is_can_start_without_time_limit(self):
        t = Thread(target=self.model.update)
        t.start()
        time.sleep(0.1)
        self.assertEqual(self.model.is_updating(), True)
        self.assertEqual(self.model.is_can_start(), False)
        time.sleep(0.3)
        self.assertEqual(self.model.is_updating(), True)
        self.assertEqual(self.model.is_can_start(), False)
        time.sleep(0.2)
        self.assertEqual(self.model.is_updating(), False)
        self.assertEqual(self.model.is_can_start(), True)
        t.join()
        
    def test_is_updating_and_is_can_start_with_time_limit(self):
        self.model.time_limit = timedelta(seconds=0.3)
        t = Thread(target=self.model.update)
        t.start()
        time.sleep(0.1)
        self.assertEqual(self.model.is_updating(), True)
        self.assertEqual(self.model.is_can_start(), False)
        time.sleep(0.3)
        self.assertEqual(self.model.is_updating(), True)
        self.assertEqual(self.model.is_can_start(), True)
        time.sleep(0.2)
        self.assertEqual(self.model.is_updating(), False)
        self.assertEqual(self.model.is_can_start(), True)
        t.join()

    def update_and_check_result(self, is_ok_expected, exc_type_expected, exc2_type_expected):
        is_ok, exc, exc2 = self.model.update()
        self.assertEqual(is_ok, is_ok_expected)
        self.assertEqual(type(exc), exc_type_expected)
        self.assertEqual(type(exc2), exc2_type_expected)

    def test_update_result_without_time_limit(self):
        t1 = Thread(target=self.update_and_check_result, args=[True, type(None), type(None)])
        t1.start()
        time.sleep(0.1)
        t2 = Thread(target=self.update_and_check_result, args=[False, type(None), type(None)])
        t2.start()
        time.sleep(0.3)
        t3 = Thread(target=self.update_and_check_result, args=[False, type(None), type(None)])
        t3.start()
        time.sleep(0.2)
        t4 = Thread(target=self.update_and_check_result, args=[True, type(None), type(None)])
        t4.start()
        for t in [t1, t2, t3, t4]:
            t.join()

    def test_update_result_with_time_limit(self):
        self.model.time_limit = timedelta(seconds=0.3)
        t1 = Thread(target=self.update_and_check_result, args=[False, ProcessTerminatedError, type(None)])
        t1.start()
        time.sleep(0.1)
        t2 = Thread(target=self.update_and_check_result, args=[False, type(None), type(None)])
        t2.start()
        time.sleep(0.3)
        t3 = Thread(target=self.update_and_check_result, args=[True, type(None), type(None)])
        t3.start()
        time.sleep(0.2)
        t4 = Thread(target=self.update_and_check_result, args=[False, type(None), type(None)])
        t4.start()
        for t in [t1, t2, t3, t4]:
            t.join()


class UpdatableLoggableModelTestCase(DefaultTestCase):
    model = UpdatableLoggableModelImpl
    abstract_model = UpdatableLoggableModel

    def assertLogValid(self, log, canceled, terminated, failed):
        self.assertTrue(
            log.was_canceled is bool(canceled)
            and log.was_terminated is bool(terminated)
            and log.has_failed is bool(failed)
        )

    def test_saving_logs_and_returning_last_update_time_and_average_update_time(self):
        mod = self.model
        mod.time_limit = timedelta(seconds=2)
        t1 = Thread(target=mod.update)  # successful
        t1.start()
        time.sleep(1)
        t2 = Thread(target=mod.update)  # canceled
        t2.start()
        time.sleep(5)
        t3 = Thread(target=mod.update)  # terminated
        t3.start()
        time.sleep(3)
        t4 = Thread(target=mod.update, kwargs={mod.shorten_update_param: True})  # successful shorter
        t4.start()
        time.sleep(2)
        t5 = Thread(target=mod.update, kwargs={mod.fail_update_param: True})  # failed
        t5.start()
        for t in [t1, t2, t3, t4, t5]:
            t.join()
        self.assertEqual(mod.log_model.objects.count(), 5)
        logs = mod.log_model.objects.order_by(*mod.log_model.get_ordering())
        self.assertLogValid(logs[0], 0, 0, 0)
        self.assertLogValid(logs[1], 1, 0, 0)
        self.assertLogValid(logs[2], 0, 1, 0)
        self.assertLogValid(logs[3], 0, 0, 0)
        self.assertLogValid(logs[4], 0, 0, 1)
        self.assertEqual(mod.get_last_update_attempt_time(), logs[4].time_finished)
        self.assertEqual(mod.get_last_successful_update_time(), logs[3].time_finished)
        self.assertFalse(None in logs.values_list(mod.log_model.time_finished.field.name, flat=True))
        self.assertTrue(2 < mod.calc_average_update_time().seconds < 4)


class HistoryModelTestCase(DefaultTestCase):
    model = HistoryModelImpl

    def assertPopulationValid(self, qs, values):
        self.assertQuerysetEqual(qs.values_list(self.model.population.field.name, flat=True), values)

    def test_objects_for_date(self):
        mod = self.model
        objs = [
            mod(**{
                mod.country.field.name: a[0],
                mod.city.field.name: a[1],
                mod.population.field.name: a[2],
                mod.date_info.field.name: a[3],
            })
            for a in [
                ['A', 'x', 10, datetime(2010, 2, 1)],
                ['A', 'y', 20, datetime(2010, 2, 1)],
                ['B', 'x', 30, datetime(2010, 2, 1)],
                ['B', 'z', 40, datetime(2010, 2, 1)],
                ['A', 'x', 11, datetime(2010, 2, 3)],
                ['A', 'y', 22, datetime(2010, 2, 3)],
                ['B', 'x', 33, datetime(2010, 2, 4)],
                ['A', 'x', 12, datetime(2010, 2, 5)],
            ]
        ]
        mod.objects.bulk_create(objs)
        self.assertPopulationValid(mod.get_objects_for_date(datetime(2010, 1, 1)), [])
        self.assertPopulationValid(mod.get_objects_for_date(datetime(2010, 2, 1)), [10, 20, 30, 40])
        self.assertPopulationValid(mod.get_objects_for_date(datetime(2010, 2, 2)), [10, 20, 30, 40])
        self.assertPopulationValid(mod.get_objects_for_date(datetime(2010, 2, 3)), [11, 22, 30, 40])
        self.assertPopulationValid(mod.get_objects_for_date(datetime(2010, 2, 4)), [11, 22, 33, 40])
        self.assertPopulationValid(mod.get_objects_for_date(datetime(2010, 2, 5)), [12, 22, 33, 40])
        self.assertPopulationValid(mod.get_objects_for_date(datetime(2010, 2, 6)), [12, 22, 33, 40])
        self.assertPopulationValid(mod.get_objects_for_date(), [12, 22, 33, 40])
