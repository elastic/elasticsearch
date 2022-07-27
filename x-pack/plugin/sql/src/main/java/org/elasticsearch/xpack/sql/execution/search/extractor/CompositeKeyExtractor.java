/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;
import org.elasticsearch.xpack.sql.querydsl.container.GroupByRef.Property;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.index.VersionCompatibilityChecks.INTRODUCING_UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.toUnsignedLong;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isDateBased;

public class CompositeKeyExtractor implements BucketExtractor {

    /**
     * Key or Composite extractor.
     */
    static final String NAME = "k";

    private final String key;
    private final Property property;
    private final ZoneId zoneId;
    private final DataType dataType;

    /**
     * Constructs a new <code>CompositeKeyExtractor</code> instance.
     */
    public CompositeKeyExtractor(String key, Property property, ZoneId zoneId, DataType dataType) {
        this.key = key;
        this.property = property;
        this.zoneId = zoneId;
        this.dataType = dataType;
    }

    CompositeKeyExtractor(StreamInput in) throws IOException {
        key = in.readString();
        property = in.readEnum(Property.class);
        if (in.getVersion().onOrAfter(Version.fromId(INTRODUCING_UNSIGNED_LONG.id))) {
            dataType = SqlDataTypes.fromTypeName(in.readString());
        } else {
            // for pre-UNSIGNED_LONG versions, the only relevant fact about the dataType was if this isDateBased() or not.
            dataType = in.readBoolean() ? DATETIME : NULL;
        }

        zoneId = SqlStreamInput.asSqlStream(in).zoneId();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(key);
        out.writeEnum(property);
        if (out.getVersion().onOrAfter(Version.fromId(INTRODUCING_UNSIGNED_LONG.id))) {
            out.writeString(dataType.typeName());
        } else {
            out.writeBoolean(isDateBased(dataType));
        }
    }

    String key() {
        return key;
    }

    Property property() {
        return property;
    }

    ZoneId zoneId() {
        return zoneId;
    }

    public DataType dataType() {
        return dataType;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object extract(Bucket bucket) {
        if (property == Property.COUNT) {
            return bucket.getDocCount();
        }
        // get the composite value
        Object m = bucket.getKey();

        if ((m instanceof Map) == false) {
            throw new SqlIllegalArgumentException("Unexpected bucket returned: {}", m);
        }

        Object object = ((Map<?, ?>) m).get(key);

        if (object != null) {
            if (isDateBased(dataType)) {
                if (object instanceof Long l) {
                    object = DateUtils.asDateTimeWithMillis(l, zoneId);
                } else {
                    throw new SqlIllegalArgumentException("Invalid date key returned: {}", object);
                }
            } else if (dataType == UNSIGNED_LONG) {
                // For integral types we coerce the bucket type to long in composite aggs (unsigned_long is not an available choice). So
                // when getting back a long value, this needs to be type- and value-converted to an UNSIGNED_LONG
                if (object instanceof Number number) {
                    object = toUnsignedLong(number);
                } else {
                    throw new SqlIllegalArgumentException("Invalid unsigned_long key returned: {}", object);
                }
            }
        }

        return object;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, property, zoneId, dataType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CompositeKeyExtractor other = (CompositeKeyExtractor) obj;
        return Objects.equals(key, other.key)
            && Objects.equals(property, other.property)
            && Objects.equals(zoneId, other.zoneId)
            && Objects.equals(dataType, other.dataType);
    }

    @Override
    public String toString() {
        return "|" + key + "|";
    }
}
