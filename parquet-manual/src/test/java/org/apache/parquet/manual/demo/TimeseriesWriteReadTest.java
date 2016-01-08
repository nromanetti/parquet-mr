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
import org.apache.parquet.manual.demo.timeseries.read.TimeseriesParquetReader;
import org.apache.parquet.manual.demo.timeseries.write.TimeseriesParquetWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TimeseriesWriteReadTest {

  @Test
  public void test() throws IOException {
    Path path = new Path("target/timeseries-list-" + System.nanoTime() + ".parquet");

    Map<String, DataPoint[]> map1 = new HashMap<String, DataPoint[]>();
    map1.put("one-ts", new DataPoint[]{new DataPoint(1, 1.1d), new DataPoint(2, 1.2d)});
    map1.put("another-ts", new DataPoint[]{new DataPoint(1, 2.1d), new DataPoint(2, 2.2d)});
    map1.put("third-ts", new DataPoint[]{new DataPoint(1, 3.1d), new DataPoint(2, 3.2d)});

    List<Timeseries> timeseriesList = new ArrayList<Timeseries>();
    timeseriesList.add(new Timeseries(map1));

    // write the timeseries list to a parquet file
    new TimeseriesParquetWriter().writeTimeseriesList(path, timeseriesList);

    // and now read it back from the same file.
    List<Timeseries> res = new TimeseriesParquetReader().readTimeseriesList(path);
    assertThat(res.size(), is(1));

    Timeseries timeseries = res.get(0);

    DataPoint[] array1 = timeseries.byName("one-ts");
    assertThat(array1[0].date, is(1));
    assertThat(array1[0].value, is(1.1d));
    assertThat(array1[1].date, is(2));
    assertThat(array1[1].value, is(1.2d));

    DataPoint[] array2 = timeseries.byName("another-ts");
    assertThat(array2[0].date, is(1));
    assertThat(array2[0].value, is(2.1d));
    assertThat(array2[1].date, is(2));
    assertThat(array2[1].value, is(2.2d));

    DataPoint[] array3 = timeseries.byName("third-ts");
    assertThat(array3[0].date, is(1));
    assertThat(array3[0].value, is(3.1d));
    assertThat(array3[1].date, is(2));
    assertThat(array3[1].value, is(3.2d));
  }
}
