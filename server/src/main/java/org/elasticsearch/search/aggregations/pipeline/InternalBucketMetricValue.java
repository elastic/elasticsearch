/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalBucketMetricValue extends InternalNumericMetricsAggregation.SingleValue implements BucketMetricValue {
    public static final String NAME = "bucket_metric_value";
    static final ParseField KEYS_FIELD = new ParseField("keys");

    private final double value;
    private final String[] keys;

    public InternalBucketMetricValue(String name, String[] keys, double value, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.keys = keys;
        this.value = value;
    }

    /**
     * Read from a stream.
     */
    public InternalBucketMetricValue(StreamInput in) throws IOException {
        super(in);
        value = in.readDouble();
        keys = in.readStringArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(value);
        out.writeStringArray(keys);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public double value() {
        return value;
    }

    @Override
    public String[] keys() {
        return keys;
    }

    DocValueFormat formatter() {
        return format;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return value();
        } else if (path.size() == 1 && KEYS_FIELD.getPreferredName().equals(path.get(0))) {
            return keys();
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = Double.isInfinite(value) == false;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? value : null);
        if (hasValue && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(value).toString());
        }
        builder.array(KEYS_FIELD.getPreferredName(), keys);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value, Arrays.hashCode(keys));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalBucketMetricValue other = (InternalBucketMetricValue) obj;
        return Objects.equals(value, other.value) && Arrays.equals(keys, other.keys);
    }
}
