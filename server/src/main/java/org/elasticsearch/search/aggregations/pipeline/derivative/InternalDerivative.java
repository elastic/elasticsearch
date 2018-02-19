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

package org.elasticsearch.search.aggregations.pipeline.derivative;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalDerivative extends InternalSimpleValue implements Derivative {
    private final double normalizationFactor;

    public InternalDerivative(String name, double value, double normalizationFactor, DocValueFormat formatter,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, value, formatter, pipelineAggregators, metaData);
        this.normalizationFactor = normalizationFactor;
    }

    /**
     * Read from a stream.
     */
    public InternalDerivative(StreamInput in) throws IOException {
        super(in);
        normalizationFactor = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeDouble(normalizationFactor);
    }

    @Override
    public String getWriteableName() {
        return DerivativePipelineAggregationBuilder.NAME;
    }

    @Override
    public double normalizedValue() {
        return normalizationFactor > 0 ? (value() / normalizationFactor) : value();
    }

    DocValueFormat formatter() {
        return format;
    }

    double getNormalizationFactor() {
        return normalizationFactor;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return value();
        } else if (path.size() == 1 && "normalized_value".equals(path.get(0))) {
            return normalizedValue();
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        super.doXContentBody(builder, params);

        if (normalizationFactor > 0) {
            boolean hasValue = !(Double.isInfinite(normalizedValue()) || Double.isNaN(normalizedValue()));
            builder.field("normalized_value", hasValue ? normalizedValue() : null);
            if (hasValue && format != DocValueFormat.RAW) {
                builder.field("normalized_value_as_string", format.format(normalizedValue()));
            }
        }
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(normalizationFactor, value);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalDerivative other = (InternalDerivative) obj;
        return Objects.equals(value, other.value)
                && Objects.equals(normalizationFactor, other.normalizationFactor);
    }
}
