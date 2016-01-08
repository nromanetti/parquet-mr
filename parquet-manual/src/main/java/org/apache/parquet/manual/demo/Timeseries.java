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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * Sample object (with a map field) for our demo whose purpose is to demonstrate how to manually store an object
 * into a parquet file and how to read it back from it.
 */
public class Timeseries {
  private Map<String, DataPoint[]> map;

  public Timeseries(Map<String, DataPoint[]> map) {
    this.map = map;
  }

  public Set<String> names() {
    return map.keySet();
  }

  public DataPoint[] byName(String name) {
    return map.get(name);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String key : names()) {
      sb.append("key: " + key + " value: " + Arrays.toString(map.get(key))).append("\n");
    }
    return sb.toString();
  }
}
