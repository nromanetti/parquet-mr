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
package org.apache.parquet.manual.demo.timeseries.write;

import org.apache.parquet.manual.demo.DataPoint;
import org.apache.parquet.manual.demo.Timeseries;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class TimeseriesWriteSupport extends WriteSupport<Timeseries> {

    private RecordConsumer recordConsumer;

    @Override
    public WriteContext init(Configuration configuration) {
        MessageType rootSchema =
                new MessageType("timeseries",
                        new GroupType(REPEATED, "map",
                                new PrimitiveType(REQUIRED, BINARY, "name"),
                                new GroupType(REPEATED, "dataPoint",
                                        new PrimitiveType(REQUIRED, INT32, "date"),
                                        new PrimitiveType(REQUIRED, DOUBLE, "value"))));


        Map<String, String> extraMetaData = new HashMap<String, String>();
        return new WriteContext(rootSchema, extraMetaData);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Timeseries timeseries) {
        recordConsumer.startMessage();
        recordConsumer.startField("map", 0);

        for (String name : timeseries.names()) {
            recordConsumer.startGroup();
            recordConsumer.startField("name", 0);
            recordConsumer.addBinary(Binary.fromString(name));
            recordConsumer.endField("name", 0);

            recordConsumer.startField("dataPoint", 1);
            for (DataPoint r : timeseries.byName(name)) {
                recordConsumer.startGroup();
                recordConsumer.startField("date", 0);
                recordConsumer.addInteger(r.date);
                recordConsumer.endField("date", 0);

                recordConsumer.startField("value", 1);
                recordConsumer.addDouble(r.value);
                recordConsumer.endField("value", 1);
                recordConsumer.endGroup();
            }
            recordConsumer.endField("dataPoint", 1);
            recordConsumer.endGroup();
        }

        recordConsumer.endField("map", 0);
        recordConsumer.endMessage();
    }
}
