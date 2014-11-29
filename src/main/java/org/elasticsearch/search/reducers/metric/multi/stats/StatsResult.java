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

package org.elasticsearch.search.reducers.metric.multi.stats;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.reducers.metric.MetricResult;

import java.io.IOException;

public class StatsResult implements MetricResult {

    private int length;
    private double sum;
    private double min;
    private double max;

    public StatsResult() {

    }
    public StatsResult(int length, double sum, double min, double max) {
        this.length = length;
        this.sum = sum;
        this.max = max;
        this.min = min;
    }
    public void readFrom(StreamInput in) throws IOException {
        length = in.readInt();
        sum = in.readDouble();
        min = in.readDouble();
        max = in.readDouble();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(length);
        out.writeDouble(sum);
        out.writeDouble(min);
        out.writeDouble(max);
    }

    public String getType() {
        return "stats_metric";
    }

    public double getValue(String name) {
        if (name.equals("length")) {
            return length;
        }
        if (name.equals("sum")) {
            return sum;
        }
        if (name.equals("min")) {
            return min;
        }
        if (name.equals("max")) {
            return max;
        }
        throw new IllegalArgumentException("stats reducer only computes length, sum, min and max. " + name + " is not supported");
    }

    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("length", length);
        builder.field("sum", sum);
        builder.field("min", min);
        builder.field("max", max);
        return builder;
    }

    @Override
    public double getValue() {
        throw new IllegalArgumentException("don't know which value you want.");
    }
}
