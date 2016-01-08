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

import org.apache.parquet.manual.demo.datapoint.read.DataPointBuilder;
import org.apache.parquet.manual.demo.DataPoint;
import org.apache.parquet.manual.demo.Timeseries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimeseriesBuilder {
    public final DataPointBuilder dataPointBuilder = new DataPointBuilder();

    private Map<String, DataPoint[]> map = new HashMap();

    String name;
    List<DataPoint> dataPoints = new ArrayList<DataPoint>();

    public void addCurrentDataPoint() {
        dataPoints.add(dataPointBuilder.build());
    }

    public void putCurrentMapEntry() {
        map.put(name, dataPoints.toArray(new DataPoint[dataPoints.size()]));
        dataPoints.clear();
        name = null;
    }

    public Timeseries build() {
        try {
            return new Timeseries(map);
        } finally {
            map = new HashMap<String, DataPoint[]>();
            dataPoints.clear();
            name = null;
        }
    }
}
