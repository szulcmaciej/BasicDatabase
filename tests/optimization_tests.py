import time
import unittest
import matplotlib.pyplot as plt

from BasicDatabase.database import Database


class OptimizationTests(unittest.TestCase):
    def test_set_time_with_n_keys_in_db(self):
        # given
        number_of_elements_in_db = range(1, 100000, 10000)

        def operation_function(elements_in_db):
            db = Database()
            for i in range(elements_in_db):
                db.set(i, i)
            start = time.time()
            for i in range(100):
                db.set('foo', 'bar')
            elapsed = time.time() - start
            return elapsed

        times = self.time_operations_for_many_iterations(operation_function, number_of_elements_in_db)

        plt.plot(number_of_elements_in_db, times)
        plt.title('setting n values')
        plt.xlabel('number_of_elements_in_db')
        plt.ylabel('execution time [s]')
        plt.show()

    def test_get_time_with_n_keys_in_db(self):
        # given
        number_of_elements_in_db = range(1, 1000000, 50000)

        def operation_function(elements_in_db):
            db = Database()
            for i in range(elements_in_db):
                db.set(i, i)
            db.set('foo', 'bar')
            start = time.time()
            for i in range(100):
                db.get('foo')
            elapsed = time.time() - start
            return elapsed

        times = self.time_operations_for_many_iterations(operation_function, number_of_elements_in_db)

        plt.plot(number_of_elements_in_db, times)
        plt.title('setting n values')
        plt.xlabel('number_of_elements_in_db')
        plt.ylabel('execution time [s]')
        plt.show()

    def test_num_equal_to_time_with_n_keys_in_db(self):
        # given
        number_of_elements_in_db = range(1, 1000000, 50000)

        def operation_function(elements_in_db):
            db = Database()
            for i in range(elements_in_db):
                db.set(i, i)
            start = time.time()
            for i in range(100):
                db.num_equal_to(10)
            elapsed = time.time() - start
            return elapsed

        times = self.time_operations_for_many_iterations(operation_function, number_of_elements_in_db)

        plt.plot(number_of_elements_in_db, times)
        plt.title('setting n values')
        plt.xlabel('number_of_elements_in_db')
        plt.ylabel('execution time [s]')
        plt.show()

    def time_operations_for_many_iterations(self, operation_function, list_of_number_of_elements_in_db):
        all_elapsed_times = []
        for iterations in list_of_number_of_elements_in_db:
            elapsed = operation_function(iterations)
            all_elapsed_times.append(elapsed)
        return all_elapsed_times
