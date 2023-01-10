/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.aggregations.metric.InternalMatrixStats;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalStats;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.aggregations.metric.MatrixAggregationInspectionHelper.hasValue;
import static org.elasticsearch.search.aggregations.support.AggregationInspectionHelper.hasValue;
import static org.elasticsearch.xpack.sql.type.SqlDataTypeConverter.convert;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isDateBased;

public class MetricAggExtractor implements BucketExtractor {

    static final String NAME = "m";

    private final String name;
    private final String property;
    private final String innerKey;
    private final DataType dataType;
    private final ZoneId zoneId;

    public MetricAggExtractor(String name, String property, String innerKey, ZoneId zoneId, @Nullable DataType dataType) {
        this.name = name;
        this.property = property;
        this.innerKey = innerKey;
        this.zoneId = zoneId;
        this.dataType = dataType;
    }

    public MetricAggExtractor(String name, String property, String innerKey, ZoneId zoneId) {
        this(name, property, innerKey, zoneId, null);
    }

    MetricAggExtractor(StreamInput in) throws IOException {
        name = in.readString();
        property = in.readString();
        innerKey = in.readOptionalString();
        String typeName = in.readOptionalString();
        if (typeName != null) {
            dataType = SqlDataTypes.fromTypeName(typeName);
        } else {
            dataType = null;
        }

        zoneId = SqlStreamInput.asSqlStream(in).zoneId();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(property);
        out.writeOptionalString(innerKey);
        out.writeOptionalString(dataType == null ? null : dataType.name());
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

    ZoneId zoneId() {
        return zoneId;
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

        if (containsValues(agg) == false) {
            return null;
        }

        if (agg instanceof InternalNumericMetricsAggregation.MultiValue) {
            // TODO: need to investigate when this can be not-null
            // if (innerKey == null) {
            // throw new SqlIllegalArgumentException("Invalid innerKey {} specified for aggregation {}", innerKey, name);
            // }
            return handleTargetType(((InternalNumericMetricsAggregation.MultiValue) agg).value(property));
        } else if (agg instanceof InternalFilter) {
            // COUNT(expr) and COUNT(ALL expr) uses this type of aggregation to account for non-null values only
            return ((InternalFilter) agg).getDocCount();
        } else if (agg instanceof InternalCardinality) {
            return ((InternalCardinality) agg).getValue();
        }

        Object v = agg.getProperty(property);
        return handleTargetType(innerKey != null && v instanceof Map ? ((Map<?, ?>) v).get(innerKey) : v);
    }

    private Object handleTargetType(Object object) {
        if (object instanceof Number && dataType != null) {
            if (isDateBased(dataType)) {
                return DateUtils.asDateTimeWithMillis(((Number) object).longValue(), zoneId);
            } else if (dataType.isInteger()) {
                // MIN and MAX need to return the same type as field's and SUM a long for integral types, but ES returns them always as
                // floating points -> convert them in the the SELECT pipeline, if needed
                return convert(object, dataType);
            }
        }
        return object;
    }

    /**
     * Check if the given aggregate has been executed and has computed values
     * or not (the bucket is null).
     */
    private static boolean containsValues(InternalAggregation agg) {
        // Stats & ExtendedStats
        if (agg instanceof InternalStats) {
            return hasValue((InternalStats) agg);
        }
        if (agg instanceof InternalMatrixStats) {
            return hasValue((InternalMatrixStats) agg);
        }
        if (agg instanceof Max) {
            return hasValue((Max) agg);
        }
        if (agg instanceof Min) {
            return hasValue((Min) agg);
        }
        if (agg instanceof InternalAvg) {
            return hasValue((InternalAvg) agg);
        }
        if (agg instanceof Sum) {
            return hasValue((Sum) agg);
        }
        if (agg instanceof InternalTDigestPercentileRanks) {
            return hasValue((InternalTDigestPercentileRanks) agg);
        }
        if (agg instanceof InternalTDigestPercentiles) {
            return hasValue((InternalTDigestPercentiles) agg);
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
        return Objects.equals(name, other.name) && Objects.equals(property, other.property) && Objects.equals(innerKey, other.innerKey);
    }

    @Override
    public String toString() {
        String i = innerKey != null ? "[" + innerKey + "]" : "";
        return Aggs.ROOT_GROUP_NAME + ">" + name + "." + property + i;
    }
}
