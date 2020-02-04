/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalTopMetrics extends InternalNumericMetricsAggregation.MultiValue {
    private final DocValueFormat sortFormat;
    private final SortOrder sortOrder;
    private final SortValue sortValue;
    private final String metricName;
    private final double metricValue;

    public InternalTopMetrics(String name, DocValueFormat sortFormat, @Nullable SortOrder sortOrder, SortValue sortValue, String metricName,
            double metricValue, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.sortFormat = sortFormat;
        this.sortOrder = sortOrder;
        this.sortValue = sortValue;
        this.metricName = metricName;
        this.metricValue = metricValue;
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
        if (false == isMapped()) {
            return this;
        }
        DocValueFormat bestSortFormat = sortFormat;
        SortValue bestSortValue = sortValue;
        double bestMetricValue = metricValue;
        int reverseMul = sortOrder.reverseMul();
        for (InternalAggregation agg : aggregations) {
            InternalTopMetrics result = (InternalTopMetrics) agg;
            if (result.sortValue != null && reverseMul * bestSortValue.compareTo(result.sortValue) > 0) {
                bestSortFormat = result.sortFormat;
                bestSortValue = result.sortValue;
                bestMetricValue = result.metricValue;
            }
        }
        return new InternalTopMetrics(getName(), bestSortFormat, sortOrder, bestSortValue, metricName, bestMetricValue,
                pipelineAggregators(), getMetaData());
    }

    @Override
    public boolean isMapped() {
        return sortValue != null;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("top");
        if (sortValue != null) {
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
            builder.endObject();
        }
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
            Objects.equals(sortValue, other.sortValue) &&
            metricName.equals(other.metricName) &&
            metricValue == other.metricValue;
    }

    @Override
    public double value(String name) {
        if (metricName.equals(name)) {
            return metricValue;
        }
        throw new IllegalArgumentException("known metric [" + name + "]");
    }

    DocValueFormat getSortFormat() {
        return sortFormat;
    }

    SortOrder getSortOrder() {
        return sortOrder;
    }

    SortValue getSortValue() {
        return sortValue;
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
}
