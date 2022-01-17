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

package org.apache.iotdb.db.query.aggregation;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.aggregation.impl.ValidityAggrResult;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Unit tests of desc aggregate result. */
public class DescAggregateResultTest {



  @Test
  public void validityMergeTest() throws QueryProcessException, IOException {
    AggregateResult ValidityAggrResult =
            AggregateResultFactory.getAggrResultByName(SQLConstant.VALIDITY, TSDataType.DOUBLE, false);


    BatchData batchData = BatchDataFactory.createBatchData(TSDataType.DOUBLE, false, false);
    batchData.putDouble(1, 1.0d);
    batchData.putDouble(2, 2.0d);
    batchData.putDouble(3, 3.0d);
    batchData.putDouble(4, 4.0d);
    batchData.putDouble(5, 5.0d);
    batchData.resetBatchData();
    IBatchDataIterator it = batchData.getBatchDataIterator();
    ValidityAggrResult.updateResultFromPageData(it);
    Assert.assertEquals(1L, ValidityAggrResult.getResult());
  }
}
