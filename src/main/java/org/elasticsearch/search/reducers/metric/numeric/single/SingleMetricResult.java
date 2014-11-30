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

package org.elasticsearch.search.reducers.metric.numeric.single;


import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.reducers.metric.numeric.NumericMetricResult;

import java.io.IOException;
import java.util.List;

public class SingleMetricResult extends NumericMetricResult {

    private double value;

    public SingleMetricResult() {
    }

    public SingleMetricResult(double value) {
        this.value = value;
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        value = in.readDouble();
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeDouble(value);
    }

    public String getType() {
        return "single_metric";
    }

    public double getValue(String name) {
        return value;
    }

    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(InternalAggregation.CommonFields.VALUE, value);
        return builder;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return getValue();
        } else {
            throw new ElasticsearchIllegalArgumentException("path not supported for [" + getType() + "]: " + path);
        }
    }

    public double getValue() {

        return value;
    }
}
