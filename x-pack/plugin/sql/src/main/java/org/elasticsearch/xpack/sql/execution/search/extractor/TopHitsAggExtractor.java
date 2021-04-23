/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.util.DateUtils;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.DATE;

public class TopHitsAggExtractor implements BucketExtractor {

    static final String NAME = "th";

    private final String name;
    private final DataType fieldDataType;
    private final ZoneId zoneId;

    public TopHitsAggExtractor(String name, DataType fieldDataType, ZoneId zoneId) {
        this.name = name;
        this.fieldDataType = fieldDataType;
        this.zoneId = zoneId;
    }

    TopHitsAggExtractor(StreamInput in) throws IOException {
        name = in.readString();
        fieldDataType = SqlDataTypes.fromTypeName(in.readString());
        zoneId = SqlStreamInput.asSqlStream(in).zoneId();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(fieldDataType.typeName());
    }

    String name() {
        return name;
    }

    DataType fieldDataType() {
        return fieldDataType;
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
        InternalTopHits agg = bucket.getAggregations().get(name);
        if (agg == null) {
            throw new SqlIllegalArgumentException("Cannot find an aggregation named {}", name);
        }

        if (agg.getHits().getTotalHits() == null || agg.getHits().getTotalHits().value == 0) {
            return null;
        }

        Object value = agg.getHits().getAt(0).getFields().values().iterator().next().getValue();
        if (fieldDataType == DATETIME || fieldDataType == DATE) {
            return DateUtils.asDateTimeWithNanos(value.toString()).withZoneSameInstant(zoneId());
        } else if (SqlDataTypes.isTimeBased(fieldDataType)) {
            return DateUtils.asTimeOnly(Long.parseLong(value.toString()), zoneId);
        } else {
            return value;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldDataType, zoneId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TopHitsAggExtractor other = (TopHitsAggExtractor) obj;
        return Objects.equals(name, other.name)
            && Objects.equals(fieldDataType, other.fieldDataType)
            && Objects.equals(zoneId, other.zoneId);
    }

    @Override
    public String toString() {
        return "TopHits>" + name + "[" + fieldDataType + "]@" + zoneId;
    }
}
