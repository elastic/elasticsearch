/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStats;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.InternalStats;
import org.elasticsearch.search.aggregations.metrics.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class MetricAggExtractor implements BucketExtractor {

    static final String NAME = "m";

    private final String name;
    private final String property;
    private final String innerKey;

    public MetricAggExtractor(String name, String property, String innerKey) {
        this.name = name;
        this.property = property;
        this.innerKey = innerKey;
    }

    MetricAggExtractor(StreamInput in) throws IOException {
        name = in.readString();
        property = in.readString();
        innerKey = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(property);
        out.writeOptionalString(innerKey);
    }

    String name() {
        return name;
    }

    String property() {
        return property;
    }

    String innerKey() {
        return innerKey;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object extract(Bucket bucket) {
        InternalAggregation agg = bucket.getAggregations().get(name);
        if (agg == null) {
            throw new SqlIllegalArgumentException("Cannot find an aggregation named {}", name);
        }

        if (!containsValues(agg)) {
            return null;
        }

        if (agg instanceof InternalNumericMetricsAggregation.MultiValue) {
            //TODO: need to investigate when this can be not-null
            //if (innerKey == null) {
            //    throw new SqlIllegalArgumentException("Invalid innerKey {} specified for aggregation {}", innerKey, name);
            //}
            return ((InternalNumericMetricsAggregation.MultiValue) agg).value(property);
        } else if (agg instanceof InternalFilter) {
            // COUNT(expr) and COUNT(ALL expr) uses this type of aggregation to account for non-null values only
            return ((InternalFilter) agg).getDocCount();
        }

        Object v = agg.getProperty(property);
        return innerKey != null && v instanceof Map ? ((Map<?, ?>) v).get(innerKey) : v;
    }

    /**
     * Check if the given aggregate has been executed and has computed values
     * or not (the bucket is null).
     * 
     * Waiting on https://github.com/elastic/elasticsearch/issues/34903
     */
    private static boolean containsValues(InternalAggregation agg) {
        // Stats & ExtendedStats
        if (agg instanceof InternalStats) {
            return ((InternalStats) agg).getCount() != 0;
        }
        if (agg instanceof MatrixStats) {
            return ((MatrixStats) agg).getDocCount() != 0;
        }
        // sum returns 0 even for null; since that's a common case, we return it as such
        if (agg instanceof SingleValue) {
            return Double.isFinite(((SingleValue) agg).value());
        }
        if (agg instanceof PercentileRanks) {
            return Double.isFinite(((PercentileRanks) agg).percent(0));
        }
        if (agg instanceof Percentiles) {
            return Double.isFinite(((Percentiles) agg).percentile(0));
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, property, innerKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        MetricAggExtractor other = (MetricAggExtractor) obj;
        return Objects.equals(name, other.name)
                && Objects.equals(property, other.property)
                && Objects.equals(innerKey, other.innerKey);
    }

    @Override
    public String toString() {
        String i = innerKey != null ? "[" + innerKey + "]" : "";
        return Aggs.ROOT_GROUP_NAME + ">" + name + "." + property + i;
    }
}