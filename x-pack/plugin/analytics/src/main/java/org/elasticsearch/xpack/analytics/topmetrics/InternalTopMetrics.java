/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalTopMetrics extends InternalAggregation {
    public static List<NamedWriteableRegistry.Entry> writeables() {
        return Arrays.asList(
                new NamedWriteableRegistry.Entry(SortValue.class, DoubleSortValue.NAME, DoubleSortValue::new),
                new NamedWriteableRegistry.Entry(SortValue.class, LongSortValue.NAME, LongSortValue::new));
    }

    private final DocValueFormat sortFormat;
    private final SortOrder sortOrder;
    private final SortValue sortValue;
    private final String metricName;
    private final double metricValue;

    private InternalTopMetrics(String name, DocValueFormat sortFormat, SortOrder sortOrder, SortValue sortValue, String metricName,
            double metricValue, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.sortFormat = sortFormat;
        this.sortOrder = sortOrder;
        this.sortValue = sortValue;
        this.metricName = metricName;
        this.metricValue = metricValue;
    }

    InternalTopMetrics(String name, DocValueFormat sortFormat, SortOrder sortOrder, Object sortValue, String metricName,
            double metricValue, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        this(name, sortFormat, sortOrder, sortValueFor(sortValue), metricName, metricValue, pipelineAggregators, metaData);
    }

    static InternalTopMetrics buildEmptyAggregation(String name, String metricField,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalTopMetrics(name, DocValueFormat.RAW, SortOrder.ASC, null, metricField, Double.NaN, pipelineAggregators,
                metaData);
    }

    /**
     * Read from a stream.
     */
    public InternalTopMetrics(StreamInput in) throws IOException {
        super(in);
        sortFormat = in.readNamedWriteable(DocValueFormat.class);
        sortOrder = SortOrder.readFromStream(in);
        sortValue = in.readOptionalNamedWriteable(SortValue.class);
        metricName = in.readString();
        metricValue = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(sortFormat);
        sortOrder.writeTo(out);
        out.writeOptionalNamedWriteable(sortValue);
        out.writeString(metricName);
        out.writeDouble(metricValue);
    }

    @Override
    public String getWriteableName() {
        return TopMetricsAggregationBuilder.NAME;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        }
        if (path.size() == 1 && metricName.contentEquals(path.get(1))) {
            return metricValue;
        }
        throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
    }

    @Override
    public InternalTopMetrics reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Iterator<InternalAggregation> itr = aggregations.iterator();
        InternalTopMetrics first;
        do {
            if (false == itr.hasNext()) {
                // All of the aggregations are empty.
                return buildEmptyAggregation(name, metricName, pipelineAggregators(), getMetaData());
            }
            first = (InternalTopMetrics) itr.next();
        } while (first.sortValue == null);
        DocValueFormat bestSortFormat = first.sortFormat;
        SortValue bestSortValue = first.sortValue;
        double bestMetricValue = first.metricValue;
        int reverseMul = first.sortOrder.reverseMul();
        while (itr.hasNext()) {
            InternalTopMetrics result = (InternalTopMetrics) itr.next();
            if (result.sortValue == null) {
                // Don't bother checking empty results.
                continue;
            }
            if (reverseMul * bestSortValue.compareTo(result.sortValue) > 0) {
                bestSortFormat = result.sortFormat;
                bestSortValue = result.sortValue;
                bestMetricValue = result.metricValue;
            }
        }
        return new InternalTopMetrics(getName(), bestSortFormat, first.sortOrder, bestSortValue, metricName, bestMetricValue,
                pipelineAggregators(), getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("top");
        builder.startObject();
        {
            builder.startArray("sort");
            if (sortFormat == DocValueFormat.RAW) {
                builder.value(sortValue.getKey());
            } else {
                builder.value(sortValue.format(sortFormat));
            }
            builder.endArray();
            builder.startObject("metrics");
            {
                builder.field(metricName, Double.isNaN(metricValue) ? null : metricValue);
            }
            builder.endObject();
        }
        builder.endObject();
        builder.endArray();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sortFormat, sortOrder, sortValue, metricName, metricValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) return false;
        InternalTopMetrics other = (InternalTopMetrics) obj;
        return sortFormat.equals(other.sortFormat) &&
            sortOrder.equals(other.sortOrder) &&
            sortValue.equals(other.sortValue) &&
            metricName.equals(other.metricName) &&
            metricValue == other.metricValue;
    }

    DocValueFormat getSortFormat() {
        return sortFormat;
    }

    SortOrder getSortOrder() {
        return sortOrder;
    }

    Object getSortValue() {
        return sortValue == null ? null : sortValue.getKey();
    }

    String getFormattedSortValue() {
        return sortValue.format(sortFormat);
    }

    String getMetricName() {
        return metricName;
    }

    double getMetricValue() {
        return metricValue;
    }

    private static SortValue sortValueFor(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Double) {
            return new DoubleSortValue((double) o);
        }
        if (o instanceof Float) {
            return new DoubleSortValue((float) o);
        }
        if (o instanceof Long) {
            return new LongSortValue((long) o);
        }
        throw new UnsupportedOperationException("no support for non-long or double keys but got [" + o.getClass() + "]");
    }
    abstract static class SortValue implements NamedWriteable, Comparable<SortValue> {
        abstract String format(DocValueFormat format);

        @Override
        public final int compareTo(SortValue other) {
            int typeCompare = getWriteableName().compareTo(other.getWriteableName());
            if (typeCompare != 0) {
                return typeCompare;
            }
            return compareToSameType(other);
        }

        protected abstract Object getKey();

        protected abstract int compareToSameType(SortValue obj);
    }

    private static class DoubleSortValue extends SortValue {
        public static final String NAME = "double";

        private final double key;

        private DoubleSortValue(double key) {
            this.key = key;
        }

        private DoubleSortValue(StreamInput in) throws IOException {
            this.key = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(key);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        protected Object getKey() {
            return key;
        }

        @Override
        String format(DocValueFormat format) {
            return format.format(key).toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            DoubleSortValue other = (DoubleSortValue) obj;
            return key == other.key;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(key);
        }

        @Override
        protected int compareToSameType(SortValue obj) {
            DoubleSortValue other = (DoubleSortValue) obj;
            return Double.compare(key, other.key);
        }

        @Override
        public String toString() {
            return Double.toString(key);
        }
    }
    private static class LongSortValue extends SortValue {
        public static final String NAME = "long";

        private final long key;

        LongSortValue(long key) {
            this.key = key;
        }

        LongSortValue(StreamInput in) throws IOException {
            key = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(key);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        protected Object getKey() {
            return key;
        }

        @Override
        String format(DocValueFormat format) {
            return format.format(key).toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            LongSortValue other = (LongSortValue) obj;
            return key == other.key;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(key);
        }

        @Override
        protected int compareToSameType(SortValue obj) {
            LongSortValue other = (LongSortValue) obj;
            return Double.compare(key, other.key);
        }

        @Override
        public String toString() {
            return Long.toString(key);
        }
    }
}
