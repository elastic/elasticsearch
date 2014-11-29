/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.reducers.metric;


import org.elasticsearch.common.io.stream.Streamable;

import java.util.HashMap;
import java.util.Map;

public abstract class MetricOp {
    protected String name;
    protected Map<String, Object> parameters = new HashMap<>();

    public abstract MetricResult evaluate(Object[] objects);

    public MetricOp(String name, Map<String, Object> parameters) {
        this.name = name;
        this.parameters = parameters;
    }

    public MetricOp(String name) {
        this.name = name;
    }

    String name() {
        return name;
    }

    Map<String, Object> parameters() {
        return parameters;
    }
}
