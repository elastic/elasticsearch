/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchSortValues;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalTopMetrics extends InternalAggregation {
    private final DocValueFormat sortFormat;
    private final SortOrder sortOrder;
    private final double sortValue;
    private final String metricName;
    private final double metricValue;

    InternalTopMetrics(String name, DocValueFormat sortFormat, SortOrder sortOrder, double sortValue, String metricName,
            double metricValue, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.sortFormat = sortFormat;
        this.sortOrder = sortOrder;
        this.sortValue = sortValue;
        this.metricName = metricName;
        this.metricValue = metricValue;
    }

    static InternalTopMetrics buildEmptyAggregation(String name, DocValueFormat sortFormat, SortOrder sortOrder, String metricField,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalTopMetrics(name, sortFormat, sortOrder, Double.NaN, metricField, Double.NaN, pipelineAggregators, metaData);
    }

    /**
     * Read from a stream.
     */
    public InternalTopMetrics(StreamInput in) throws IOException {
        super(in);
        sortFormat = in.readNamedWriteable(DocValueFormat.class);
        sortOrder = SortOrder.readFromStream(in);
        sortValue = in.readDouble();
        metricName = in.readString();
        metricValue = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(sortFormat);
        sortOrder.writeTo(out);
        out.writeDouble(sortValue);
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
        while (true) {
            if (false == itr.hasNext()) {
                // Reducing a bunch of empty aggregations
                return buildReduced(DocValueFormat.RAW, Double.NaN, Double.NaN);
            }
            first = (InternalTopMetrics) itr.next();
            // Results with NaN sorts are empty
            if (false == Double.isNaN(first.sortValue)) {
                break;
            }
        }
        DocValueFormat bestSortFormat = first.sortFormat;
        double bestSortValue = first.sortValue;
        double bestMetricValue = first.metricValue;
        int reverseMul = sortOrder == SortOrder.DESC ? -1 : 1;
        while (itr.hasNext()) {
            InternalTopMetrics result = (InternalTopMetrics) itr.next();
            // Results with NaN sorts are empty
            if (Double.isNaN(result.sortValue)) {
                continue;
            }
            if (reverseMul * Double.compare(bestSortValue, result.sortValue) > 0) {
                bestSortFormat = result.sortFormat;
                bestSortValue = result.sortValue;
                bestMetricValue = result.metricValue;
            }
        }
        return buildReduced(bestSortFormat, bestSortValue, bestMetricValue);
    }

    private InternalTopMetrics buildReduced(DocValueFormat bestSortFormat, double bestSortValue, double bestMetricValue) {
        return new InternalTopMetrics(getName(), bestSortFormat, sortOrder, bestSortValue, metricName, bestMetricValue,
                pipelineAggregators(), getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("top");
        builder.startObject();
        {
            // Sadly, this won't output dates correctly because they always come back as doubles. We 
            SearchSortValues sortValues = new SearchSortValues(new Object[] {sortValue}, new DocValueFormat[] {sortFormat});
            sortValues.toXContent(builder, params);
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
            sortValue == other.sortValue &&
            metricName.equals(other.metricName) &&
            metricValue == other.metricValue;
    }

    DocValueFormat getSortFormat() {
        return sortFormat;
    }

    SortOrder getSortOrder() {
        return sortOrder;
    }

    double getSortValue() {
        return sortValue;
    }

    String getMetricName() {
        return metricName;
    }

    double getMetricValue() {
        return metricValue;
    }
}
