/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Derivative extends InternalSimpleValue {
    private final double normalizationFactor;

    Derivative(String name, double value, double normalizationFactor, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, value, formatter, metadata);
        this.normalizationFactor = normalizationFactor;
    }

    /**
     * Read from a stream.
     */
    public Derivative(StreamInput in) throws IOException {
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

    /**
     * Returns the normalized value. If no normalised factor has been specified
     * this method will return {@link #value()}
     *
     * @return the normalized value
     */
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
            boolean hasValue = (Double.isInfinite(normalizedValue()) || Double.isNaN(normalizedValue())) == false;
            builder.field("normalized_value", hasValue ? normalizedValue() : null);
            if (hasValue && format != DocValueFormat.RAW) {
                builder.field("normalized_value_as_string", format.format(normalizedValue()));
            }
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), normalizationFactor, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        Derivative other = (Derivative) obj;
        return Objects.equals(value, other.value) && Objects.equals(normalizationFactor, other.normalizationFactor);
    }
}
