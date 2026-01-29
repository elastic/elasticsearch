/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.startree;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a grouping field configuration for a star-tree index.
 * Grouping fields define the hierarchical structure of the star-tree.
 * They can be regular fields (keyword, numeric) or date_histogram fields.
 */
public final class StarTreeGroupingField implements Writeable, ToXContentObject {

    private static final String FIELD_PARAM = "field";
    private static final String TYPE_PARAM = "type";
    private static final String INTERVAL_PARAM = "interval";

    public enum GroupingType {
        /**
         * Regular grouping using the field's values directly (keyword, numeric).
         */
        ORDINAL,
        /**
         * Date histogram grouping that buckets timestamps.
         */
        DATE_HISTOGRAM;

        public static GroupingType fromString(String value) {
            try {
                return GroupingType.valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "Unknown star-tree grouping field type [" + value + "]. Valid values are: [ordinal, date_histogram]"
                );
            }
        }

        public String toXContentValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final String field;
    private final GroupingType type;
    @Nullable
    private final TimeValue interval;

    public StarTreeGroupingField(String field, GroupingType type, @Nullable TimeValue interval) {
        if (Strings.isNullOrEmpty(field)) {
            throw new IllegalArgumentException("Star-tree grouping field cannot be null or empty");
        }
        this.field = field;
        this.type = Objects.requireNonNull(type, "Star-tree grouping field type cannot be null");
        this.interval = interval;

        if (type == GroupingType.DATE_HISTOGRAM && interval == null) {
            throw new IllegalArgumentException("Star-tree date_histogram grouping field requires an interval");
        }
        if (type == GroupingType.ORDINAL && interval != null) {
            throw new IllegalArgumentException("Star-tree ordinal grouping field does not support interval");
        }
    }

    public StarTreeGroupingField(StreamInput in) throws IOException {
        this.field = in.readString();
        this.type = in.readEnum(GroupingType.class);
        this.interval = in.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeEnum(type);
        out.writeOptionalTimeValue(interval);
    }

    public String getField() {
        return field;
    }

    public GroupingType getType() {
        return type;
    }

    @Nullable
    public TimeValue getInterval() {
        return interval;
    }

    /**
     * Returns the interval in milliseconds for date_histogram grouping fields, or -1 for ordinal fields.
     */
    public long getIntervalMillis() {
        return interval != null ? interval.millis() : -1;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_PARAM, field);
        if (type != GroupingType.ORDINAL) {
            builder.field(TYPE_PARAM, type.toXContentValue());
        }
        if (interval != null) {
            builder.field(INTERVAL_PARAM, interval.getStringRep());
        }
        builder.endObject();
        return builder;
    }

    /**
     * Parse a grouping field from XContent.
     */
    @SuppressWarnings("unchecked")
    public static StarTreeGroupingField parse(Object fieldNode) {
        if (fieldNode instanceof Map == false) {
            throw new MapperParsingException("Star-tree grouping field must be an object");
        }

        Map<String, Object> fieldMap = (Map<String, Object>) fieldNode;
        String field = null;
        GroupingType type = GroupingType.ORDINAL;
        TimeValue interval = null;

        for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
            String paramName = entry.getKey();
            Object paramValue = entry.getValue();

            switch (paramName) {
                case FIELD_PARAM -> {
                    if (paramValue instanceof String == false) {
                        throw new MapperParsingException("Star-tree grouping field name must be a string");
                    }
                    field = (String) paramValue;
                }
                case TYPE_PARAM -> {
                    if (paramValue instanceof String == false) {
                        throw new MapperParsingException("Star-tree grouping field type must be a string");
                    }
                    type = GroupingType.fromString((String) paramValue);
                }
                case INTERVAL_PARAM -> {
                    if (paramValue instanceof String) {
                        interval = TimeValue.parseTimeValue((String) paramValue, INTERVAL_PARAM);
                    } else if (paramValue instanceof Number) {
                        interval = TimeValue.timeValueMillis(((Number) paramValue).longValue());
                    } else {
                        throw new MapperParsingException("Star-tree grouping field interval must be a string or number");
                    }
                }
                default -> throw new MapperParsingException("Unknown star-tree grouping field parameter [" + paramName + "]");
            }
        }

        if (field == null) {
            throw new MapperParsingException("Star-tree grouping field must specify a field");
        }

        return new StarTreeGroupingField(field, type, interval);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StarTreeGroupingField that = (StarTreeGroupingField) o;
        return Objects.equals(field, that.field) && type == that.type && Objects.equals(interval, that.interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, type, interval);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
