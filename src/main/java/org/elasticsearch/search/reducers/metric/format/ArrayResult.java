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

package org.elasticsearch.search.reducers.metric.format;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.reducers.metric.MetricResult;

import java.io.IOException;
import java.util.List;

public class ArrayResult implements MetricResult {

    private double[] values;

    public ArrayResult() {
    }

    public ArrayResult(double[] value) {
        this.values = value;
    }

    public void readFrom(StreamInput in) throws IOException {
        values = in.readDoubleArray();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(values);
    }

    public String getType() {
        return Array.TYPE;
    }

    public double[] getArray() {
        return values;
    }

    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(InternalAggregation.CommonFields.VALUE, values);
        return builder;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.size() == 0) {
            return this;
        }
        if (path.get(0).equals("array")) {
            return getArray();
        }
        throw new IllegalArgumentException("array reducer only has array. " + path + " is not supported");
    }
}
