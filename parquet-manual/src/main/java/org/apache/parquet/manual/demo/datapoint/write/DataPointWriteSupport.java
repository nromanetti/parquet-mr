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
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.manual.demo.DataPoint;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class DataPointWriteSupport extends WriteSupport<DataPoint> {
  public final static MessageType rootSchema = new MessageType("dataPoint",
      new PrimitiveType(REQUIRED, INT32, "date"),
      new PrimitiveType(REQUIRED, DOUBLE, "value"));

  private RecordConsumer recordConsumer;

  @Override
  public WriteContext init(Configuration configuration) {
    Map<String, String> extraMetaData = new HashMap<String, String>();
    return new WriteContext(rootSchema, extraMetaData);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(DataPoint record) {
    recordConsumer.startMessage();

    recordConsumer.startField("date", 0);
    recordConsumer.addInteger(record.date);
    recordConsumer.endField("date", 0);

    recordConsumer.startField("value", 1);
    recordConsumer.addDouble(record.value);
    recordConsumer.endField("value", 1);

    recordConsumer.endMessage();
  }
}
