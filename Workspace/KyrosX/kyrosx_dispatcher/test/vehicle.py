#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest

import KyrosUtils

class TestMyModule(unittest.TestCase):

    def test_sync_system_user(self):
        vehicle = KyrosUtils.vehicle.get_vehicle_from_id(4)
        self.assertEqual(vehicle.device_id, 4)

if __name__ == "__main__":
    unittest.main()
