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

package org.elasticsearch.search.reducers.metric.multi.delta;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.reducers.metric.MetricResult;

import java.io.IOException;

public class DeltaResult implements MetricResult {

    private double value;

    public DeltaResult() {
    }
    public DeltaResult(double value) {
        this.value = value;
    }
    public void readFrom(StreamInput in) throws IOException {
        value = in.readDouble();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(value);
    }

    public String getType() {
        return "delta_metric";
    }

    public double getValue(String name) {
        if (name.equals("delta")) {
            return value;
        }
        throw new IllegalArgumentException("delta reducer only computes delta. " + name + " is not supported");

    }

    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(InternalAggregation.CommonFields.VALUE, value);
        return builder;
    }

    @Override
    public double getValue() {
        throw new IllegalArgumentException("don't know which value you want.");
    }
}
