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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class InternalTopMetrics extends InternalNumericMetricsAggregation.MultiValue {
    private final SortOrder sortOrder;
    private final int size;
    private final String metricName;
    private final List<TopMetric> topMetrics;

    public InternalTopMetrics(String name, @Nullable SortOrder sortOrder, String metricName,
            int size, List<TopMetric> topMetrics, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.sortOrder = sortOrder;
        this.metricName = metricName;
        /*
         * topMetrics.size won't be size when the bucket doesn't have size docs!
         */
        this.size = size;
        this.topMetrics = topMetrics;
    }

    static InternalTopMetrics buildEmptyAggregation(String name, String metricField,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalTopMetrics(name, SortOrder.ASC, metricField, 0, emptyList(), pipelineAggregators, metaData);
    }

    /**
     * Read from a stream.
     */
    public InternalTopMetrics(StreamInput in) throws IOException {
        super(in);
        sortOrder = SortOrder.readFromStream(in);
        metricName = in.readString();
        size = in.readVInt();
        topMetrics = in.readList(TopMetric::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        sortOrder.writeTo(out);
        out.writeString(metricName);
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
        if (path.size() == 1 && metricName.contentEquals(path.get(1))) {
            if (topMetrics.isEmpty()) {
                // Unmapped.
                return null;
            }
            assert topMetrics.size() == 1 : "property paths should only resolve against top metrics with size == 1.";
            return topMetrics.get(0).metricValue;
        }
        throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
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
        return new InternalTopMetrics(getName(), sortOrder, metricName, size, merged, pipelineAggregators(), getMetaData());
    }

    @Override
    public boolean isMapped() {
        return false == topMetrics.isEmpty();
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("top");
        for (TopMetric top : topMetrics) {
            top.toXContent(builder, metricName);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sortOrder, metricName, size, topMetrics);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) return false;
        InternalTopMetrics other = (InternalTopMetrics) obj;
        return sortOrder.equals(other.sortOrder) &&
            metricName.equals(other.metricName) &&
            size == other.size &&
            topMetrics.equals(other.topMetrics);
    }

    @Override
    public double value(String name) {
        if (metricName.equals(name)) {
            if (topMetrics.isEmpty()) {
                return Double.NaN;
            }
            assert topMetrics.size() == 1 : "property paths should only resolve against top metrics with size == 1.";
            return topMetrics.get(0).metricValue;
        }
        throw new IllegalArgumentException("known metric [" + name + "]");
    }

    SortOrder getSortOrder() {
        return sortOrder;
    }

    int getSize() {
        return size;
    }

    String getMetricName() {
        return metricName;
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
        private final double metricValue;

        TopMetric(DocValueFormat sortFormat, SortValue sortValue, double metricValue) {
            this.sortFormat = sortFormat;
            this.sortValue = sortValue;
            this.metricValue = metricValue;
        }

        TopMetric(StreamInput in) throws IOException {
            sortFormat = in.readNamedWriteable(DocValueFormat.class);
            sortValue = in.readNamedWriteable(SortValue.class);
            metricValue = in.readDouble();
        }

        DocValueFormat getSortFormat() {
            return sortFormat;
        }

        SortValue getSortValue() {
            return sortValue;
        }

        double getMetricValue() {
            return metricValue;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(sortFormat);
            out.writeNamedWriteable(sortValue);
            out.writeDouble(metricValue);
        }

        public XContentBuilder toXContent(XContentBuilder builder, String metricName) throws IOException {
            builder.startObject();
            {
                builder.startArray("sort");
                sortValue.toXContent(builder, sortFormat);
                builder.endArray();
                builder.startObject("metrics");
                {
                    builder.field(metricName, Double.isNaN(metricValue) ? null : metricValue);
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
                    && metricValue == other.metricValue;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sortFormat, sortValue, metricValue);
        }

        @Override
        public String toString() {
            return "TopMetric[" + sortFormat + "," + sortValue + "," + metricValue + "]"; 
        }
    }
}
