/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.manual.demo;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.manual.demo.datapoint.read.DataPointParquetReader;
import org.apache.parquet.manual.demo.datapoint.write.DataPointParquetWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DataPointWriteReadTest {

  @Test
  public void test() throws IOException {
    Path path = new Path("target/datapoint-list-" + System.nanoTime() + ".parquet");

    List<DataPoint> dataPointList = new ArrayList<DataPoint>();
    dataPointList.add(new DataPoint(0, 0.0d));
    dataPointList.add(new DataPoint(1, 1.1d));
    dataPointList.add(new DataPoint(2, 2.2d));

    // write the list to a parquet file
    new DataPointParquetWriter().writeDataPointList(path, dataPointList);

    // and now read it from the same file
    List<DataPoint> res = new DataPointParquetReader().readDataPointList(path);

    assertThat(res.size(), is(3));

    assertThat(res.get(0).date, is(0));
    assertThat(res.get(0).value, is(0.0d));

    assertThat(res.get(1).date, is(1));
    assertThat(res.get(1).value, is(1.1d));

    assertThat(res.get(2).date, is(2));
    assertThat(res.get(2).value, is(2.2d));
  }
}
