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

import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Objects;

/**
 * This class is used for recording statistic information of each measurement in a delta file. While
 * writing processing, the processor records the statistics information. Statistics includes
 * maximum, minimum and null value count up to version 0.0.1.<br>
 * Each data type extends this Statistic as super class.<br>
 * <br>
 * For the statistics in the Unseq file TimeSeriesMetadata, only firstValue, lastValue, startTime
 * and endTime can be used.</br>
 */
public abstract class Statistics<T extends Serializable> {

  private static final Logger LOG = LoggerFactory.getLogger(Statistics.class);
  /**
   * isEmpty being false means this statistic has been initialized and the max and min is not null;
   */
  protected boolean isEmpty = true;

  protected boolean validityMerge = true;
  /** number of time-value points */
  private int count = 0;

  private int indexEnd = 0;
  private int indexLastRepaired = -1;
  private double validityErrors = 0;
  private double speedAVG = 0;
  private double speedSTD = 0;
  private int windowSize = 20;
  private LinkedList<Long> firstTimeWindow = new LinkedList<>();
  private LinkedList<Double> firstValueWindow = new LinkedList<>();
  private LinkedList<Long> timeWindow = new LinkedList<>();
  private LinkedList<Double> valueWindow = new LinkedList<>();
  private LinkedList<Integer> repairedIndex = new LinkedList<>();
  private LinkedList<Integer> repairedSize = new LinkedList<>();
  private long startTime = Long.MAX_VALUE;
  private long endTime = Long.MIN_VALUE;

  /**
   * static method providing statistic instance for respective data type.
   *
   * @param type - data type
   * @return Statistics
   */
  public static Statistics<? extends Serializable> getStatsByType(TSDataType type) {
    switch (type) {
      case INT32:
        return new IntegerStatistics();
      case INT64:
        return new LongStatistics();
      case TEXT:
        return new BinaryStatistics();
      case BOOLEAN:
        return new BooleanStatistics();
      case DOUBLE:
        return new DoubleStatistics();
      case FLOAT:
        return new FloatStatistics();
      case VECTOR:
        return new TimeStatistics();
      default:
        throw new UnknownColumnTypeException(type.toString());
    }
  }

  public static int getSizeByType(TSDataType type) {
    switch (type) {
      case INT32:
        return IntegerStatistics.INTEGER_STATISTICS_FIXED_RAM_SIZE;
      case INT64:
        return LongStatistics.LONG_STATISTICS_FIXED_RAM_SIZE;
      case TEXT:
        return BinaryStatistics.BINARY_STATISTICS_FIXED_RAM_SIZE;
      case BOOLEAN:
        return BooleanStatistics.BOOLEAN_STATISTICS_FIXED_RAM_SIZE;
      case DOUBLE:
        return DoubleStatistics.DOUBLE_STATISTICS_FIXED_RAM_SIZE;
      case FLOAT:
        return FloatStatistics.FLOAT_STATISTICS_FIXED_RAM_SIZE;
      case VECTOR:
        return TimeStatistics.TIME_STATISTICS_FIXED_RAM_SIZE;
      default:
        throw new UnknownColumnTypeException(type.toString());
    }
  }

  public abstract TSDataType getType();

  public int getSerializedSize() {
    return ReadWriteForEncodingUtils.uVarIntSize(count) // count
        + 16 // startTime, endTime
        + 24 // validity, speed max, speed min
        + getStatsSize();
  }

  public abstract int getStatsSize();

  public int serialize(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(count, outputStream);
    byteLen += ReadWriteIOUtils.write(startTime, outputStream);
    byteLen += ReadWriteIOUtils.write(endTime, outputStream);
    byteLen += ReadWriteIOUtils.write(validityErrors, outputStream);
    byteLen += ReadWriteIOUtils.write(speedAVG, outputStream);
    byteLen += ReadWriteIOUtils.write(speedSTD, outputStream);
    // value statistics of different data type
    byteLen += serializeStats(outputStream);
    return byteLen;
  }

  abstract int serializeStats(OutputStream outputStream) throws IOException;

  /** read data from the inputStream. */
  public abstract void deserialize(InputStream inputStream) throws IOException;

  public abstract void deserialize(ByteBuffer byteBuffer);

  public abstract void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes);

  public abstract T getMinValue();

  public abstract T getMaxValue();

  public abstract T getFirstValue();

  public abstract T getLastValue();

  public abstract double getSumDoubleValue();

  public abstract long getSumLongValue();

  public abstract byte[] getMinValueBytes();

  public abstract byte[] getMaxValueBytes();

  public abstract byte[] getFirstValueBytes();

  public abstract byte[] getLastValueBytes();

  public abstract byte[] getSumValueBytes();

  public abstract ByteBuffer getMinValueBuffer();

  public abstract ByteBuffer getMaxValueBuffer();

  public abstract ByteBuffer getFirstValueBuffer();

  public abstract ByteBuffer getLastValueBuffer();

  public abstract ByteBuffer getSumValueBuffer();

  /**
   * merge parameter to this statistic
   *
   * @throws StatisticsClassException cannot merge statistics
   */
  @SuppressWarnings("unchecked")
  public void mergeStatistics(Statistics<? extends Serializable> stats) {

    if (this.getClass() == stats.getClass()) {
      if (this.count == 0) {
        this.timeWindow = stats.timeWindow;
        this.valueWindow = stats.valueWindow;
        this.firstValueWindow = stats.firstValueWindow;
        this.firstTimeWindow = stats.firstTimeWindow;
      } else {
        if (stats.firstTimeWindow.size() < windowSize) {
          for (int i = 0; i < stats.firstTimeWindow.size(); i++) {
            update(stats.firstTimeWindow.get(i), stats.firstValueWindow.get(i));
          }
          return;
        }
      }
      if (stats.startTime < this.startTime) {
        this.startTime = stats.startTime;
        int length = stats.firstTimeWindow.size();
        for (int i = length; i <= windowSize; i++) {}

        this.firstValueWindow = stats.firstValueWindow;
        this.firstTimeWindow = stats.firstTimeWindow;
      }
      if (stats.endTime > this.endTime) {
        this.endTime = stats.endTime;
      }
      // must be sure no overlap between two statistics
      this.count += stats.count;
      this.speedAVG = Math.min(stats.speedAVG, this.speedAVG);
      this.speedSTD = Math.min(stats.speedSTD, this.speedSTD);
      this.validityErrors += stats.validityErrors;
      mergeStatisticsValue((Statistics<T>) stats);
      isEmpty = false;
    } else {
      Class<?> thisClass = this.getClass();
      Class<?> statsClass = stats.getClass();
      LOG.warn("Statistics classes mismatched,no merge: {} v.s. {}", thisClass, statsClass);

      throw new StatisticsClassException(thisClass, statsClass);
    }
  }

  public void update(long time, boolean value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, int value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, long value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, float value) {
    update(time);
    updateStats(value);
  }

  // 更新Validity
  public void update(long time, double value) {
    update(time);
    updateStats(value);

    double smax = this.speedAVG + 3 * this.speedSTD;
    double smin = this.speedAVG - 3 * this.speedSTD;

    // update window
    if (timeWindow.size() < windowSize) {
      int index = timeWindow.size();
      timeWindow.add(time);
      valueWindow.add(value);
      firstTimeWindow.add(time);
      firstValueWindow.add(value);
      indexEnd = index;
      if (index > 0) {
        double timeLastInterval = timeWindow.get(index) - timeWindow.get(index - 1);
        if (timeLastInterval != 0) {
          double speedNow =
              (valueWindow.get(index) - valueWindow.get(index - 1)) / timeLastInterval;
          updateAVGSTD(speedNow);
          smax = this.speedAVG + 3 * this.speedSTD;
          smin = this.speedAVG - 3 * this.speedSTD;
          if ((speedNow < smin || speedNow > smax) && index < windowSize / 2) {
            validityErrors += 1;
          }
        }
      }

    } else if ((count - indexLastRepaired) > windowSize / 2 + 1) {
      timeWindow.pollFirst();
      valueWindow.pollFirst();
      timeWindow.add(time);
      valueWindow.add(value);
      double speedLast = 0;
      double speedNow = 0;

      int index = windowSize - 1;
      if (index > 0) {
        double timeLastInterval = timeWindow.get(index) - timeWindow.get(index - 1);
        if (timeLastInterval != 0) {
          speedLast = (valueWindow.get(index) - valueWindow.get(index - 1)) / timeLastInterval;
          updateAVGSTD(speedLast);
        }
      }
      double timeInterval = (timeWindow.get(windowSize / 2) - timeWindow.get(windowSize / 2 - 1));
      if (timeInterval != 0) {
        speedNow =
            (valueWindow.get(windowSize / 2) - valueWindow.get(windowSize / 2 - 1)) / timeInterval;
      } else {
        return;
      }
      //      Test used
      //      smin = -2;
      //      smax = +2;

      if (speedNow > smax || speedNow < smin) {
        int needRepairIndex = windowSize / 2;
        int startScanIndex = needRepairIndex - 1;
        int stopScanIndex = needRepairIndex + 1;
        this.validityErrors +=
            repairSize(smin, smax, startScanIndex, stopScanIndex, indexLastRepaired);
      }
    } else {
      timeWindow.pollFirst();
      valueWindow.pollFirst();
      timeWindow.add(time);
      valueWindow.add(value);
      int index = windowSize - 1;
      if (index > 0) {
        double timeLastInterval = timeWindow.get(index) - timeWindow.get(index - 1);
        if (timeLastInterval != 0) {
          double speedLast =
              (valueWindow.get(index) - valueWindow.get(index - 1)) / timeLastInterval;
          updateAVGSTD(speedLast);
        }
      }
    }
  }

  public int repairSize(
      double smin, double smax, int firstScanIndex, int lastScanIndex, int indexLastRepaired) {
    int lastRepairIndex = windowSize - (count - indexLastRepaired);
    for (int i = 0; i < windowSize / 2; i++) {
      for (int j = 0; j < i; j++) {
        int startIndex = firstScanIndex - j;
        int stopIndex = lastScanIndex + (i - j);
        if (startIndex > 0 && startIndex > lastRepairIndex && stopIndex < timeWindow.size()) {
          long timeInterval = timeWindow.get(stopIndex) - timeWindow.get(startIndex);
          double valueInterval = valueWindow.get(stopIndex) - valueWindow.get(startIndex);
          double speedNow = valueInterval / timeInterval;
          if (speedNow <= smax && speedNow >= smin) {
            this.indexLastRepaired = count - stopIndex + 1;
            return stopIndex - startIndex - 1;
          }
        }
      }
    }
    return 0;
  }

  public void updateAVGSTD(double speedNow) {
    speedSTD =
        (count - 1) / Math.pow(count, 2) * Math.pow(speedNow - speedAVG, 2)
            + (double) (count - 1) / count * speedSTD;
    speedAVG = speedAVG + (speedNow - speedAVG) / count;
  }

  public void update(long time, Binary value) {
    update(time);
    updateStats(value);
  }

  public void update(long time) {
    if (time < startTime) {
      startTime = time;
    }
    if (time > endTime) {
      endTime = time;
    }
    count++;
  }

  public void update(long[] time, boolean[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, int[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, long[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, float[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, double[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, Binary[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, int batchSize) {
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    count += batchSize;
  }

  protected abstract void mergeStatisticsValue(Statistics<T> stats);

  public boolean isEmpty() {
    return isEmpty;
  }

  public void setEmpty(boolean empty) {
    isEmpty = empty;
  }

  void updateStats(boolean value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(int value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(long value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(float value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(double value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(Binary value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(boolean[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(int[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(long[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(float[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(double[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(Binary[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  /**
   * This method with two parameters is only used by {@code unsequence} which
   * updates/inserts/deletes timestamp.
   *
   * @param min min timestamp
   * @param max max timestamp
   */
  public void updateStats(long min, long max) {
    throw new UnsupportedOperationException();
  }

  public static Statistics<? extends Serializable> deserialize(
      InputStream inputStream, TSDataType dataType) throws IOException {
    Statistics<? extends Serializable> statistics = getStatsByType(dataType);
    statistics.setCount(ReadWriteForEncodingUtils.readUnsignedVarInt(inputStream));
    statistics.setStartTime(ReadWriteIOUtils.readLong(inputStream));
    statistics.setEndTime(ReadWriteIOUtils.readLong(inputStream));
    statistics.setValidityErrors(ReadWriteIOUtils.readDouble(inputStream));
    statistics.setSpeedAVG(ReadWriteIOUtils.readDouble(inputStream));
    statistics.setSpeedSTD(ReadWriteIOUtils.readDouble(inputStream));
    statistics.deserialize(inputStream);
    statistics.isEmpty = false;
    return statistics;
  }

  public static Statistics<? extends Serializable> deserialize(
      ByteBuffer buffer, TSDataType dataType) {
    Statistics<? extends Serializable> statistics = getStatsByType(dataType);
    statistics.setCount(ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
    statistics.setStartTime(ReadWriteIOUtils.readLong(buffer));
    statistics.setEndTime(ReadWriteIOUtils.readLong(buffer));
    statistics.setValidityErrors(ReadWriteIOUtils.readDouble(buffer));
    statistics.setSpeedAVG(ReadWriteIOUtils.readDouble(buffer));
    statistics.setSpeedSTD(ReadWriteIOUtils.readDouble(buffer));
    statistics.deserialize(buffer);
    statistics.isEmpty = false;
    return statistics;
  }

  public LinkedList<Long> getFirstTimeWindow() {
    return firstTimeWindow;
  }

  public void setFirstTimeWindow(LinkedList<Long> firstTimeWindow) {
    this.firstTimeWindow = firstTimeWindow;
  }

  public LinkedList<Double> getFirstValueWindow() {
    return firstValueWindow;
  }

  public void setFirstValueWindow(LinkedList<Double> firstValueWindow) {
    this.firstValueWindow = firstValueWindow;
  }

  public LinkedList<Long> getTimeWindow() {
    return timeWindow;
  }

  public void setTimeWindow(LinkedList<Long> timeWindow) {
    this.timeWindow = timeWindow;
  }

  public LinkedList<Double> getValueWindow() {
    return valueWindow;
  }

  public void setValueWindow(LinkedList<Double> valueWindow) {
    this.valueWindow = valueWindow;
  }

  public double getSpeedAVG() {
    return speedAVG;
  }

  public void setSpeedAVG(double speedAVG) {
    this.speedAVG = speedAVG;
  }

  public double getSpeedSTD() {
    return speedSTD;
  }

  public void setSpeedSTD(double speedSTD) {
    this.speedSTD = speedSTD;
  }

  public double getValidity() {
    return 1 - validityErrors / count;
  }

  public void setValidityErrors(double validityErrors) {
    this.validityErrors = validityErrors;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public long getCount() {
    return count;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public abstract long calculateRamSize();

  public boolean checkMergeable(Statistics<? extends Serializable> statisticsMerge) {
    if (this.count == 0) {
      return true;
    } else if (this.indexLastRepaired == count - 1) {
      return false;
    } else if (this.timeWindow.size() < windowSize) {
      return statisticsMerge.firstTimeWindow.size() < windowSize;
    } else if (statisticsMerge.firstTimeWindow.size() < windowSize) {
      return true;
    } else {
      double smin =
          Math.max(
              this.speedAVG - 3 * this.speedSTD,
              statisticsMerge.speedAVG - 3 * statisticsMerge.speedSTD);
      double smax =
          Math.min(
              this.speedAVG + 3 * this.speedSTD,
              statisticsMerge.speedAVG + 3 * statisticsMerge.speedSTD);
      long timeInterval =
          this.timeWindow.get(this.timeWindow.size() - 1) - statisticsMerge.firstTimeWindow.get(0);
      double valueInterval =
          this.valueWindow.get(this.timeWindow.size() - 1)
              - statisticsMerge.firstValueWindow.get(0);
      double speedInterval = valueInterval / timeInterval;
      return speedInterval < smax && speedInterval > smin;
    }
  }

  @Override
  public String toString() {
    return "startTime: "
        + startTime
        + " endTime: "
        + endTime
        + " count: "
        + count
        + "validityerrors:"
        + validityErrors;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o != null && getClass() == o.getClass();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), count, startTime, endTime);
  }
}
