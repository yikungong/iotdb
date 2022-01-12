/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.junit.Test;

import static org.junit.Assert.*;

public class DoubleStatisticsTest {

  private static final double maxError = 0.0001d;

  @Test
  public void testUpdate() {
    Statistics<Double> doubleStats = new DoubleStatistics();
    doubleStats.update(1, 1.34d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(2, 2.32d);
    assertFalse(doubleStats.isEmpty());
    assertEquals(0.5, doubleStats.getValidity(), maxError);
    assertEquals(2.32d, doubleStats.getMaxValue(), maxError);
    assertEquals(1.34d, doubleStats.getMinValue(), maxError);
    assertEquals(2.32d + 1.34d, doubleStats.getSumDoubleValue(), maxError);
    assertEquals(1.34d, doubleStats.getFirstValue(), maxError);
    assertEquals(2.32d, doubleStats.getLastValue(), maxError);
  }

  @Test
  public void testMerge() {
    Statistics<Double> doubleStats1 = new DoubleStatistics();
    doubleStats1.setStartTime(0);
    doubleStats1.setEndTime(1);
    Statistics<Double> doubleStats2 = new DoubleStatistics();
    doubleStats2.setStartTime(2);
    doubleStats2.setEndTime(5);

    doubleStats1.update(0, 1.34d);
    doubleStats1.update(1, 100.13453d);

    doubleStats2.update(2, 200.435d);
    doubleStats2.update(3, 200.435d);

    Statistics<Double> doubleStats3 = new DoubleStatistics();
    doubleStats3.mergeStatistics(doubleStats1);
    assertFalse(doubleStats3.isEmpty());
    assertEquals(0.5, doubleStats3.getValidity(), maxError);
    assertEquals(100.13453d, doubleStats3.getMaxValue(), maxError);
    assertEquals(1.34d, doubleStats3.getMinValue(), maxError);
    assertEquals(100.13453d + 1.34d, doubleStats3.getSumDoubleValue(), maxError);
    assertEquals(1.34d, doubleStats3.getFirstValue(), maxError);
    assertEquals(100.13453d, doubleStats3.getLastValue(), maxError);

    doubleStats3.mergeStatistics(doubleStats2);

    assertEquals(0.25, doubleStats3.getValidity(), maxError);
    assertEquals(200.435d, doubleStats3.getMaxValue(), maxError);
    assertEquals(1.34d, doubleStats3.getMinValue(), maxError);
    assertEquals(
        100.13453d + 1.34d + 200.435d + 200.435d, doubleStats3.getSumDoubleValue(), maxError);
    assertEquals(1.34d, doubleStats3.getFirstValue(), maxError);
    assertEquals(200.435d, doubleStats3.getLastValue(), maxError);

    // Unseq merge
    Statistics<Double> doubleStats4 = new DoubleStatistics();
    doubleStats4.setStartTime(0);
    doubleStats4.setEndTime(5);
    Statistics<Double> doubleStats5 = new DoubleStatistics();
    doubleStats5.setStartTime(1);
    doubleStats5.setEndTime(4);

    doubleStats4.updateStats(122.34d);
    doubleStats4.updateStats(125.34d);
    doubleStats5.updateStats(111.1d);

    doubleStats3.mergeStatistics(doubleStats4);
    assertEquals(122.34d, doubleStats3.getFirstValue(), maxError);
    assertEquals(125.34d, doubleStats3.getLastValue(), maxError);

    doubleStats3.mergeStatistics(doubleStats5);
    assertEquals(122.34d, doubleStats3.getFirstValue(), maxError);
    assertEquals(125.34d, doubleStats3.getLastValue(), maxError);
  }

  @Test
  public void testUpdateValidity() {
    Statistics<Double> doubleStats = new DoubleStatistics();
    Statistics<Double> doubleStatistics = new DoubleStatistics();
    Statistics<Double> doubleStatisticsEmpty = new DoubleStatistics();
    Statistics<Double> doubleStatisticsOne = new DoubleStatistics();

    doubleStats.update(1, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(2, 1.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(3, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(4, 1.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(5, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(6, 1.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(7, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(8, 1.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(9, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(10, 1.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(11, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(12, 1.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(13, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(14, 1.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(15, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(16, 1.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(17, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(18, 1.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(19, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(20, 1.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(21, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(22, 100.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(23, 101.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(24, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(25, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(26, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(27, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(28, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(29, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(30, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(31, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(32, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(33, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(34, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(35, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(36, 2.32d);
    assertFalse(doubleStats.isEmpty());
    doubleStats.update(37, 2.32d);
    assertFalse(doubleStats.isEmpty());

    doubleStatisticsOne.update(39, 2.32d);

    doubleStatistics.update(40, 2.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(41, 1.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(42, 2.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(43, 1.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(44, 2.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(45, 1.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(46, 2.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(47, 1.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(48, 2.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(49, 1.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(50, 2.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(51, 1.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(52, 2.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(53, 1.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(54, 2.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(55, 1.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(56, 2.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(57, 1.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(58, 2.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(59, 1.32d);
    assertFalse(doubleStatistics.isEmpty());
    doubleStatistics.update(60, 2.32d);
    assertFalse(doubleStatistics.isEmpty());

    assertTrue(doubleStatisticsEmpty.checkMergeable(doubleStatistics));
    assertTrue(doubleStatisticsOne.checkMergeable(doubleStatisticsOne));
    assertTrue(doubleStats.checkMergeable(doubleStatistics));
    assertEquals(1.0, doubleStats.getValidity(), maxError);
  }
}
