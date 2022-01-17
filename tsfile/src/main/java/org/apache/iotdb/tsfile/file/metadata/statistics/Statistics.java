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
import java.util.ArrayList;
import java.util.List;
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
  private int validityErrors = 0;
  private double speedAVG = 0;
  private double speedSTD = 0;
  private int windowSize = 20;
  private List<Boolean> lastRepair = new ArrayList<>();
  private List<Boolean> firstRepair = new ArrayList<>();
  private boolean repairSelfLast = true;
  private boolean repairSelfFirst = true;
  private int indexNotRepair = -1;
  private List<Long> timeWindow = new ArrayList<>();
  private List<Double> valueWindow = new ArrayList<>();
  private List<Integer> DP = new ArrayList<>();
  private List<Integer> reverseDP = new ArrayList<>();
  private long startTime = Long.MAX_VALUE;
  private long endTime = Long.MIN_VALUE;
  private double startValue;
  private double endValue;

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
        + 16 // startValue, endValue
        + 8 // repairFirst and last
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
    byteLen += ReadWriteIOUtils.write(startValue, outputStream);
    byteLen += ReadWriteIOUtils.write(endValue, outputStream);
    byteLen += ReadWriteIOUtils.write(repairSelfFirst, outputStream);
    byteLen += ReadWriteIOUtils.write(repairSelfLast, outputStream);

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
      this.timeWindow = stats.timeWindow;
      this.valueWindow = stats.valueWindow;
      this.DP = stats.DP;
      this.firstRepair = stats.firstRepair;
      this.lastRepair = stats.lastRepair;
      if (stats.startTime < this.startTime) {
        this.startValue = stats.startValue;
        this.startTime = stats.startTime;
        this.repairSelfFirst = stats.repairSelfFirst;
      }
      if (stats.endTime > this.endTime) {
        this.endValue = stats.endValue;
        this.endTime = stats.endTime;
        this.repairSelfLast = stats.repairSelfLast;
      }
      // must be sure no overlap between two statistics
      this.count += stats.count;
      this.speedAVG = stats.speedAVG;
      this.speedSTD = stats.speedSTD;
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

    double smax;
    double smin;

    // update window

    int index = timeWindow.size();
    timeWindow.add(time);
    valueWindow.add(value);
    endValue = value;
    if (index > 0) {
      double timeLastInterval = timeWindow.get(index) - timeWindow.get(index - 1);
      if (timeLastInterval != 0) {
        double speedNow = (valueWindow.get(index) - valueWindow.get(index - 1)) / timeLastInterval;
        updateAVGSTD(speedNow);
        smax = this.speedAVG + 3 * this.speedSTD;
        smin = this.speedAVG - 3 * this.speedSTD;
        updateDP(index, smax, smin);
      }
    } else {
      startValue = value;
      endValue = value;
      firstRepair.add(false);
      DP.add(0);
    }
  }

  public void updateDP(int index, double smax, double smin) {
    Long time = timeWindow.get(index);
    Double value = valueWindow.get(index);
    int dp = -1;
    boolean find = false;
    for (int i = 0; i < index; i++) {
      if ((value - valueWindow.get(i)) / (time - timeWindow.get(i)) <= smax
          && (value - valueWindow.get(i)) / (time - timeWindow.get(i)) >= smin) {
        find = true;
        if (dp == -1) {
          dp = DP.get(i) + index - i - 1;
          if (firstRepair.get(i)) {
            if (firstRepair.size() == index + 1) {
              firstRepair.set(index, true);
            } else {
              firstRepair.add(true);
            }
          } else {
            if (firstRepair.size() == index + 1) {
              firstRepair.set(index, false);
            } else {
              firstRepair.add(false);
            }
          }
        } else {
          if (DP.get(i) + index - i - 1 < dp) {
            dp = DP.get(i) + index - i - 1;
            if (firstRepair.get(i)) {
              if (firstRepair.size() == index + 1) {
                firstRepair.set(index, true);
              } else {
                firstRepair.add(true);
              }
            } else {
              if (firstRepair.size() == index + 1) {
                firstRepair.set(index, false);
              } else {
                firstRepair.add(false);
              }
            }
          }
        }
      }
    }
    if (!find) {
      dp = index;
      firstRepair.add(true);
    }
    DP.add(dp);
  }

  public void updateReverseDP(int Length, double smax, double smin) {
    lastRepair.add(false);
    reverseDP.add(0);

    for (int j = Length - 2; j >= 0; j--) {
      if (j == Length / 2) {
        System.out.println("half");
      }
      Long time = timeWindow.get(j);
      Double value = valueWindow.get(j);
      int dp = -1;
      boolean find = false;
      for (int i = Length - 1; i > j; i--) {
        int index = Length - i - 1;
        if ((value - valueWindow.get(i)) / (time - timeWindow.get(i)) <= smax
            && (value - valueWindow.get(i)) / (time - timeWindow.get(i)) >= smin) {
          find = true;
          if (dp == -1) {
            dp = reverseDP.get(index) + i - j - 1;
            if (firstRepair.get(index)) {
              if (firstRepair.size() == Length - j) {
                firstRepair.set(Length - j - 1, true);
              } else {
                firstRepair.add(true);
              }
            } else {
              if (firstRepair.size() == Length - j) {
                firstRepair.set(Length - j - 1, false);
              } else {
                firstRepair.add(false);
              }
            }
          } else {
            if (reverseDP.get(index) + i - j - 1 < dp) {
              dp = reverseDP.get(index) + i - j - 1;
              if (firstRepair.get(i)) {
                if (firstRepair.size() == Length) {
                  firstRepair.set(Length - 1, true);
                } else {
                  firstRepair.add(true);
                }
              } else {
                if (firstRepair.size() == Length) {
                  firstRepair.set(Length - 1, false);
                } else {
                  firstRepair.add(false);
                }
              }
            }
          }
        }
      }
      if (!find) {
        dp = Length - 1;
        lastRepair.add(true);
      }
      reverseDP.add(dp);
    }
    validityErrors = Length;
    for (int m = 0; m < Length; m++) {
      if (validityErrors > DP.get(m) + reverseDP.get(Length - 1 - m)) {
        validityErrors = DP.get(m) + reverseDP.get(Length - 1 - m);
        indexLastRepaired = m;
      }
    }
    if (this.firstRepair.get(this.indexLastRepaired)) {
      this.repairSelfFirst = false;
    }
    if (this.lastRepair.get(this.indexLastRepaired)) {
      this.repairSelfLast = false;
    }
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

    statistics.setValidityErrors(ReadWriteIOUtils.readInt(inputStream));
    statistics.setSpeedAVG(ReadWriteIOUtils.readDouble(inputStream));
    statistics.setSpeedSTD(ReadWriteIOUtils.readDouble(inputStream));
    statistics.setStartValue(ReadWriteIOUtils.readDouble(inputStream));
    statistics.setEndValue(ReadWriteIOUtils.readDouble(inputStream));
    statistics.setRepairSelfFirst(ReadWriteIOUtils.readBool(inputStream));
    statistics.setRepairSelfLast(ReadWriteIOUtils.readBool(inputStream));

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

    statistics.setValidityErrors(ReadWriteIOUtils.readInt(buffer));
    statistics.setSpeedAVG(ReadWriteIOUtils.readDouble(buffer));
    statistics.setSpeedSTD(ReadWriteIOUtils.readDouble(buffer));
    statistics.setStartValue(ReadWriteIOUtils.readDouble(buffer));
    statistics.setEndValue(ReadWriteIOUtils.readDouble(buffer));
    statistics.setRepairSelfFirst(ReadWriteIOUtils.readBool(buffer));
    statistics.setRepairSelfLast(ReadWriteIOUtils.readBool(buffer));

    statistics.deserialize(buffer);
    statistics.isEmpty = false;
    return statistics;
  }

  public List<Long> getTimeWindow() {
    return timeWindow;
  }

  public void setTimeWindow(List<Long> timeWindow) {
    this.timeWindow = timeWindow;
  }

  public List<Double> getValueWindow() {
    return valueWindow;
  }

  public List<Boolean> getLastRepair() {
    return lastRepair;
  }

  public List<Boolean> getFirstRepair() {
    return firstRepair;
  }

  public List<Integer> getDP() {
    return DP;
  }

  public List<Integer> getReverseDP() {
    return reverseDP;
  }

  public void setLastRepair(List<Boolean> lastRepair) {
    this.lastRepair = lastRepair;
  }

  public void setFirstRepair(List<Boolean> firstRepair) {
    this.firstRepair = firstRepair;
  }

  public void setDP(List<Integer> DP) {
    this.DP = DP;
  }

  public void setReverseDP(List<Integer> reverseDP) {
    this.reverseDP = reverseDP;
  }

  public void setValueWindow(List<Double> valueWindow) {
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

  public void setValidityErrors(int validityErrors) {
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

  public boolean isRepairSelfLast() {
    return repairSelfLast;
  }

  public void setRepairSelfLast(boolean repairSelfLast) {
    this.repairSelfLast = repairSelfLast;
  }

  public boolean isRepairSelfFirst() {
    return repairSelfFirst;
  }

  public void setRepairSelfFirst(boolean repairSelfFirst) {
    this.repairSelfFirst = repairSelfFirst;
  }

  public double getStartValue() {
    return startValue;
  }

  public void setStartValue(double startValue) {
    this.startValue = startValue;
  }

  public double getEndValue() {
    return endValue;
  }

  public void setEndValue(double endValue) {
    this.endValue = endValue;
  }

  public abstract long calculateRamSize();

  public boolean checkMergeable(Statistics<? extends Serializable> statisticsMerge) {
    if (this.count == 0) {
      return true;
    } else if (this.repairSelfLast && this.repairSelfFirst) {
      double speed =
          (this.endValue - statisticsMerge.startValue) / (this.endTime - statisticsMerge.startTime);
      double smax = this.speedAVG + 3 * this.speedSTD;
      double smin = this.speedAVG - 3 * this.speedSTD;
      return speed <= smax && speed >= smin;
    } else {
      return false;
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
