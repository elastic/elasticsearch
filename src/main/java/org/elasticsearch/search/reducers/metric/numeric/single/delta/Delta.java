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

package org.elasticsearch.search.reducers.metric.numeric.single.delta;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.reducers.ReductionExecutionException;
import org.elasticsearch.search.reducers.metric.MetricOp;
import org.elasticsearch.search.reducers.metric.MetricsBuilder;
import org.elasticsearch.search.reducers.metric.numeric.single.SingleMetricResult;

import java.io.IOException;

public class Delta extends MetricOp {

    boolean gradient = false; 

    public Delta() {
        super("delta");
    }

    public SingleMetricResult evaluate(Object[] bucketProperties) throws ReductionExecutionException {
        double firstBucketValue = ((Number) bucketProperties[0]).doubleValue();
        double lastBucketValue = ((Number) bucketProperties[bucketProperties.length - 1]).doubleValue();
        double deltaValue = lastBucketValue - firstBucketValue;
        if (this.gradient) {
            deltaValue = deltaValue / (bucketProperties.length - 1);
        }
        return new SingleMetricResult(deltaValue);
    }

    public void readFrom(StreamInput in) throws IOException {
        this.gradient = in.readBoolean();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeBoolean(gradient);
    }
    protected boolean parseParameter(String currentFieldName, XContentParser parser) throws IOException {
        if (currentFieldName.equals("gradient")) {
            gradient= parser.booleanValue();
            return true;
        }
        return false;
    }

    public static class DeltaBuilder extends MetricsBuilder {

        private boolean gradient = false;

        public DeltaBuilder(String name) {
            super(name, "delta");
        }

        public DeltaBuilder computeGradient(boolean gradient) {
            this.gradient = gradient;
            return this;
        }

        @Override
        protected XContentBuilder buildCustomParameters(XContentBuilder builder) throws IOException {
            builder.field("gradient", gradient);
            return builder;
        }
    }
}
