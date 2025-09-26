/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * An {@link ExtendedSearchUsageMetric} implementation that holds a map of values to counts.
 */
public class ExtendedSearchUsageLongCounter implements ExtendedSearchUsageMetric<ExtendedSearchUsageLongCounter> {

    public static final String NAME = "extended_search_usage_long_counter";

    private final Map<String, Long> values;

    public ExtendedSearchUsageLongCounter(Map<String, Long> values) {
        this.values = values;
    }

    public ExtendedSearchUsageLongCounter(StreamInput in) throws IOException {
        this.values = in.readMap(StreamInput::readString, StreamInput::readLong);
    }

    public Map<String, Long> getValues() {
        return Collections.unmodifiableMap(values);
    }

    @Override
    public ExtendedSearchUsageLongCounter merge(ExtendedSearchUsageMetric<?> other) {
        assert other instanceof ExtendedSearchUsageLongCounter;
        ExtendedSearchUsageLongCounter otherLongCounter = (ExtendedSearchUsageLongCounter) other;
        Map<String, Long> values = new java.util.HashMap<>(this.values);
        otherLongCounter.getValues().forEach((key, otherValue) -> { values.merge(key, otherValue, Long::sum); });
        return new ExtendedSearchUsageLongCounter(values);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(values, StreamOutput::writeString, StreamOutput::writeLong);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (String key : values.keySet()) {
            builder.field(key, values.get(key));
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtendedSearchUsageLongCounter that = (ExtendedSearchUsageLongCounter) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
