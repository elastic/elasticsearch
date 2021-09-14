/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class InternalNumericMetricsAggregation extends InternalAggregation {

    private static final DocValueFormat DEFAULT_FORMAT = DocValueFormat.RAW;

    protected DocValueFormat format = DEFAULT_FORMAT;

    public abstract static class SingleValue extends InternalNumericMetricsAggregation implements NumericMetricsAggregation.SingleValue {
        protected SingleValue(String name, Map<String, Object> metadata) {
            super(name, metadata);
        }

        /**
         * Read from a stream.
         */
        protected SingleValue(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getValueAsString() {
            return format.format(value()).toString();
        }

        @Override
        public Object getProperty(List<String> path) {
            if (path.isEmpty()) {
                return this;
            } else if (path.size() == 1 && "value".equals(path.get(0))) {
                return value();
            } else {
                throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
            }
        }

        @Override
        public final double sortValue(String key) {
            if (key != null && false == key.equals("value")) {
                throw new IllegalArgumentException(
                    "Unknown value key ["
                        + key
                        + "] for single-value metric aggregation ["
                        + getName()
                        + "]. Either use [value] as key or drop the key all together"
                );
            }
            return value();
        }
    }

    public abstract static class MultiValue extends InternalNumericMetricsAggregation implements NumericMetricsAggregation.MultiValue {
        protected MultiValue(String name, Map<String, Object> metadata) {
            super(name, metadata);
        }

        /**
         * Read from a stream.
         */
        protected MultiValue(StreamInput in) throws IOException {
            super(in);
        }

        public abstract double value(String name);

        public String valueAsString(String name) {
            return format.format(value(name)).toString();
        }

        @Override
        public Object getProperty(List<String> path) {
            if (path.isEmpty()) {
                return this;
            } else if (path.size() == 1) {
                return value(path.get(0));
            } else {
                throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
            }
        }

        @Override
        public final double sortValue(String key) {
            if (key == null) {
                throw new IllegalArgumentException("Missing value key in [" + key + "] which refers to a multi-value metric aggregation");
            }
            return value(key);
        }
    }

    private InternalNumericMetricsAggregation(String name, Map<String, Object> metadata) {
        super(name, metadata);
    }

    /**
     * Read from a stream.
     */
    protected InternalNumericMetricsAggregation(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public final double sortValue(AggregationPath.PathElement head, Iterator<AggregationPath.PathElement> tail) {
        throw new IllegalArgumentException("Metrics aggregations cannot have sub-aggregations (at [>" + head + "]");
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), format);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalNumericMetricsAggregation other = (InternalNumericMetricsAggregation) obj;
        return Objects.equals(format, other.format);
    }
}
