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
package org.apache.parquet.manual.demo.datapoint.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.manual.demo.DataPoint;

import java.io.IOException;
import java.util.List;

public class DataPointParquetWriter {

  public void writeDataPointList(Path path, List<DataPoint> dataPoints) throws IOException {
    ParquetWriter<DataPoint> writer = new Builder(path).build();

    for (DataPoint dataPoint : dataPoints) {
      writer.write(dataPoint);
    }

    writer.close();
  }

  private static class Builder extends ParquetWriter.Builder<DataPoint, Builder> {
    private Builder(Path file) {
      super(file);
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected WriteSupport<DataPoint> getWriteSupport(Configuration conf) {
      return new DataPointWriteSupport();
    }
  }
}