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
package org.apache.parquet.manual.demo.timeseries.read;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;

public class FieldMapConverter extends GroupConverter {

  private final TimeseriesBuilder timeseriesBuilder;
  private final MapKeyConverter mapKeyConverter;
  private final DataPointConverterForTimeseries dataPointConverterForTimeseries;

  public FieldMapConverter(TimeseriesBuilder timeseriesBuilder) {
    this.timeseriesBuilder = timeseriesBuilder;
    this.mapKeyConverter = new MapKeyConverter(timeseriesBuilder);
    this.dataPointConverterForTimeseries = new DataPointConverterForTimeseries(timeseriesBuilder);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    switch (fieldIndex) {
      case 0:
        return mapKeyConverter;
      case 1:
        return dataPointConverterForTimeseries;
      default:
        throw new IllegalStateException("got: " + fieldIndex);
    }
  }

  @Override
  public void start() {
    System.out.println(getClass().getSimpleName() + ".start");
  }

  @Override
  public void end() {
    System.out.println(getClass().getSimpleName() + ".end");
    timeseriesBuilder.putCurrentMapEntry();
  }
}
