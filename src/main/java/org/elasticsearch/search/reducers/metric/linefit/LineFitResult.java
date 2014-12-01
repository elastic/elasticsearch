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

package org.elasticsearch.search.reducers.metric.linefit;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.reducers.metric.MetricResult;

import java.io.IOException;
import java.util.List;

public class LineFitResult implements MetricResult {

    private double slope;
    private double bias;
    private double[] modelValues = null;

    public LineFitResult() {
    }

    public LineFitResult(double slope, double bias, double[] modelValues) {
        this.slope = slope;
        this.bias = bias;
        this.modelValues = modelValues;
    }

    public void readFrom(StreamInput in) throws IOException {
        slope = in.readDouble();
        bias = in.readDouble();
        boolean readModelValue = in.readBoolean();
        if (readModelValue) {
            modelValues = in.readDoubleArray();
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(slope);
        out.writeDouble(bias);
        if (modelValues != null) {
            out.writeBoolean(true);
            out.writeDoubleArray(modelValues);
        } else {
            out.writeBoolean(false);
        }
    }

    public String getType() {
        return LineFit.TYPE;
    }

    public double getSlope() {
        return slope;
    }

    public double getBias() {
        return bias;
    }

    public double[] getModelValues() {
        return modelValues;
    }

    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("slope", slope);
        builder.field("bias", bias);
        if (modelValues != null) {
            builder.field("model_values", modelValues);
        }
        return builder;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.size() == 0) {
            return this;
        }
        if (path.get(0).equals("slope")) {
            return slope;
        }
        if (path.get(0).equals("bias")) {
            return bias;
        }
        if (path.get(0).equals("model_values")) {
            return bias;
        }
        throw new IllegalArgumentException("line fit reducer only has slope and bias. " + path + " is not supported");
    }
}
