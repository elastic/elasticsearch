/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static org.elasticsearch.search.builder.SearchSourceBuilder.SORT_FIELD;
import static  org.elasticsearch.xpack.analytics.topmetrics.TopMetricsAggregationBuilder.METRIC_FIELD;


public class InternalTopMetrics extends InternalNumericMetricsAggregation.MultiValue {
    private final SortOrder sortOrder;
    private final int size;
    private final List<String> metricNames;
    private final List<TopMetric> topMetrics;

    public InternalTopMetrics(String name, @Nullable SortOrder sortOrder, List<String> metricNames,
            int size, List<TopMetric> topMetrics, Map<String, Object> metadata) {
        super(name, metadata);
        this.sortOrder = sortOrder;
        this.metricNames = metricNames;
        /*
         * topMetrics.size won't be size when the bucket doesn't have size docs!
         */
        this.size = size;
        this.topMetrics = topMetrics;
    }

    static InternalTopMetrics buildEmptyAggregation(String name, List<String> metricNames, Map<String, Object> metadata) {
        return new InternalTopMetrics(name, SortOrder.ASC, metricNames, 0, emptyList(), metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalTopMetrics(StreamInput in) throws IOException {
        super(in);
        sortOrder = SortOrder.readFromStream(in);
        metricNames = in.readStringList();
        size = in.readVInt();
        topMetrics = in.readList(TopMetric::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        sortOrder.writeTo(out);
        out.writeStringCollection(metricNames);
        out.writeVInt(size);
        out.writeList(topMetrics);
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
        if (path.size() != 1) {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
        int index = metricNames.indexOf(path.get(0));
        if (index < 0) {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
        if (topMetrics.isEmpty()) {
            // Unmapped.
            return null;
        }
        assert topMetrics.size() == 1 : "property paths should only resolve against top metrics with size == 1.";
        return topMetrics.get(0).metricValues.get(index).numberValue();
    }

    @Override
    public InternalTopMetrics reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        if (false == isMapped()) {
            return this;
        }
        List<TopMetric> merged = new ArrayList<>(size);
        PriorityQueue<ReduceState> queue = new PriorityQueue<ReduceState>(aggregations.size()) {
            @Override
            protected boolean lessThan(ReduceState lhs, ReduceState rhs) {
                return sortOrder.reverseMul() * lhs.sortValue().compareTo(rhs.sortValue()) < 0;
            }
        };
        for (InternalAggregation agg : aggregations) {
            InternalTopMetrics result = (InternalTopMetrics) agg;
            if (result.isMapped()) {
                queue.add(new ReduceState(result));
            }
        }
        while (queue.size() > 0 && merged.size() < size) {
            merged.add(queue.top().topMetric());
            queue.top().index++;
            if (queue.top().result.topMetrics.size() <= queue.top().index) {
                queue.pop();
            } else {
                queue.updateTop();
            }
        }
        return new InternalTopMetrics(getName(), sortOrder, metricNames, size, merged, getMetadata());
    }

    @Override
    public boolean isMapped() {
        return false == topMetrics.isEmpty();
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("top");
        for (TopMetric top : topMetrics) {
            top.toXContent(builder, metricNames);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sortOrder, metricNames, size, topMetrics);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) return false;
        InternalTopMetrics other = (InternalTopMetrics) obj;
        return sortOrder.equals(other.sortOrder) &&
            metricNames.equals(other.metricNames) &&
            size == other.size &&
            topMetrics.equals(other.topMetrics);
    }

    @Override
    public double value(String name) {
        int index = metricNames.indexOf(name);
        if (index < 0) {
            throw new IllegalArgumentException("unknown metric [" + name + "]");
        }
        if (topMetrics.isEmpty()) {
            return Double.NaN;
        }
        assert topMetrics.size() == 1 : "property paths should only resolve against top metrics with size == 1.";
        // TODO it'd probably be nicer to have "compareTo" instead of assuming a double.
        return topMetrics.get(0).metricValues.get(index).numberValue().doubleValue();
    }

    SortOrder getSortOrder() {
        return sortOrder;
    }

    int getSize() {
        return size;
    }

    List<String> getMetricNames() {
        return metricNames;
    }

    List<TopMetric> getTopMetrics() {
        return topMetrics;
    }

    private class ReduceState {
        private final InternalTopMetrics result;
        private int index = 0;

        ReduceState(InternalTopMetrics result) {
            this.result = result;
        }

        SortValue sortValue() {
            return topMetric().sortValue;
        }

        TopMetric topMetric() {
            return result.topMetrics.get(index);
        }
    }

    static class TopMetric implements Writeable, Comparable<TopMetric> {
        private final DocValueFormat sortFormat;
        private final SortValue sortValue;
        private final List<MetricValue> metricValues;

        TopMetric(DocValueFormat sortFormat, SortValue sortValue, List<MetricValue> metricValues) {
            this.sortFormat = sortFormat;
            this.sortValue = sortValue;
            this.metricValues = metricValues;
        }

        TopMetric(StreamInput in) throws IOException {
            sortFormat = in.readNamedWriteable(DocValueFormat.class);
            sortValue = in.readNamedWriteable(SortValue.class);
            metricValues = in.readList(s -> s.readOptionalWriteable(MetricValue::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(sortFormat);
            out.writeNamedWriteable(sortValue);
            out.writeCollection(metricValues, StreamOutput::writeOptionalWriteable);
        }

        DocValueFormat getSortFormat() {
            return sortFormat;
        }

        SortValue getSortValue() {
            return sortValue;
        }

        List<MetricValue> getMetricValues() {
            return metricValues;
        }

        public XContentBuilder toXContent(XContentBuilder builder, List<String> metricNames) throws IOException {
            builder.startObject();
            {
                builder.startArray(SORT_FIELD.getPreferredName());
                sortValue.toXContent(builder, sortFormat);
                builder.endArray();
                builder.startObject(METRIC_FIELD.getPreferredName());
                for (int i = 0; i < metricValues.size(); i++) {
                    MetricValue value = metricValues.get(i);
                    builder.field(metricNames.get(i));
                    if (value == null) {
                        builder.nullValue();
                    } else {
                        value.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    }
                }
                builder.endObject();
            }
            return builder.endObject();
        }

        @Override
        public int compareTo(TopMetric o) {
            return sortValue.compareTo(o.sortValue);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            TopMetric other = (TopMetric) obj;
            return sortFormat.equals(other.sortFormat)
                    && sortValue.equals(other.sortValue)
                    && metricValues.equals(other.metricValues);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sortFormat, sortValue, metricValues);
        }

        @Override
        public String toString() {
            return "TopMetric[" + sortFormat + "," + sortValue + "," + metricValues + "]";
        }
    }

    static class MetricValue implements Writeable, ToXContent {
        private final DocValueFormat format;
        /**
         * It is odd to have a "SortValue" be part of a MetricValue but it is
         * a very convenient way to send a type-aware thing across the
         * wire though. So here we are.
         */
        private final SortValue value;

        MetricValue(DocValueFormat format, SortValue value) {
            this.format = format;
            this.value = value;
        }

        DocValueFormat getFormat() {
            return format;
        }

        SortValue getValue() {
            return value;
        }

        MetricValue(StreamInput in) throws IOException {
            format = in.readNamedWriteable(DocValueFormat.class);
            value = in.readNamedWriteable(SortValue.class);
        }

        Number numberValue() {
            return value.numberValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(format);
            out.writeNamedWriteable(value);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return value.toXContent(builder, format);
        }

        @Override
        public String toString() {
            return format + "," + value;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            MetricValue other = (MetricValue) obj;
            return format.equals(other.format) && value.equals(other.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(format, value);
        }
    }
}
