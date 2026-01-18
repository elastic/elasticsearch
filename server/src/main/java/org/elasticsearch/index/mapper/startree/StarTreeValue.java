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
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a value field configuration for a star-tree index.
 * Value fields are the numeric fields that will have pre-aggregated values stored.
 * Each value field has a list of aggregation types to pre-compute.
 */
public final class StarTreeValue implements Writeable, ToXContentObject {

    private static final String FIELD_PARAM = "field";
    private static final String AGGREGATIONS_PARAM = "aggregations";

    private final String field;
    private final Set<StarTreeAggregationType> aggregations;
    private final boolean isDoubleField;

    public StarTreeValue(String field, Set<StarTreeAggregationType> aggregations) {
        this(field, aggregations, false);
    }

    public StarTreeValue(String field, Set<StarTreeAggregationType> aggregations, boolean isDoubleField) {
        if (Strings.isNullOrEmpty(field)) {
            throw new IllegalArgumentException("Star-tree value field cannot be null or empty");
        }
        if (aggregations == null || aggregations.isEmpty()) {
            throw new IllegalArgumentException("Star-tree value must have at least one aggregation type");
        }
        this.field = field;
        this.aggregations = Collections.unmodifiableSet(EnumSet.copyOf(aggregations));
        this.isDoubleField = isDoubleField;
    }

    public StarTreeValue(StreamInput in) throws IOException {
        this.field = in.readString();
        int size = in.readVInt();
        EnumSet<StarTreeAggregationType> types = EnumSet.noneOf(StarTreeAggregationType.class);
        for (int i = 0; i < size; i++) {
            types.add(in.readEnum(StarTreeAggregationType.class));
        }
        this.aggregations = Collections.unmodifiableSet(types);
        this.isDoubleField = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeVInt(aggregations.size());
        for (StarTreeAggregationType type : aggregations) {
            out.writeEnum(type);
        }
        out.writeBoolean(isDoubleField);
    }

    public String getField() {
        return field;
    }

    public Set<StarTreeAggregationType> getAggregations() {
        return aggregations;
    }

    public boolean hasAggregation(StarTreeAggregationType type) {
        return aggregations.contains(type);
    }

    /**
     * Returns true if this value field is a double/float type, false if it's a long/int type.
     * This is needed to properly decode doc values (double fields use sortableLongToDouble encoding).
     */
    public boolean isDoubleField() {
        return isDoubleField;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_PARAM, field);
        builder.startArray(AGGREGATIONS_PARAM);
        for (StarTreeAggregationType type : aggregations) {
            builder.value(type.toXContentValue());
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * Parse a value field from XContent.
     */
    public static StarTreeValue parse(Object valueNode) {
        return parse(valueNode, fieldName -> false);
    }

    /**
     * Parse a value field from XContent with a field type resolver.
     *
     * @param valueNode the XContent node to parse
     * @param isDoubleFieldResolver a function that returns true if the given field name is a double/float type
     */
    @SuppressWarnings("unchecked")
    public static StarTreeValue parse(Object valueNode, java.util.function.Function<String, Boolean> isDoubleFieldResolver) {
        if (valueNode instanceof Map == false) {
            throw new MapperParsingException("Star-tree value must be an object");
        }

        Map<String, Object> valueMap = (Map<String, Object>) valueNode;
        String field = null;
        EnumSet<StarTreeAggregationType> aggregations = EnumSet.noneOf(StarTreeAggregationType.class);

        for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
            String paramName = entry.getKey();
            Object paramValue = entry.getValue();

            switch (paramName) {
                case FIELD_PARAM -> {
                    if (paramValue instanceof String == false) {
                        throw new MapperParsingException("Star-tree value field must be a string");
                    }
                    field = (String) paramValue;
                }
                case AGGREGATIONS_PARAM -> {
                    if (paramValue instanceof List == false) {
                        throw new MapperParsingException("Star-tree value aggregations must be an array");
                    }
                    List<?> aggList = (List<?>) paramValue;
                    for (Object agg : aggList) {
                        if (agg instanceof String == false) {
                            throw new MapperParsingException("Star-tree value aggregation type must be a string");
                        }
                        aggregations.add(StarTreeAggregationType.fromString((String) agg));
                    }
                }
                default -> throw new MapperParsingException("Unknown star-tree value parameter [" + paramName + "]");
            }
        }

        if (field == null) {
            throw new MapperParsingException("Star-tree value must specify a field");
        }

        if (aggregations.isEmpty()) {
            throw new MapperParsingException("Star-tree value must specify at least one aggregation type");
        }

        boolean isDoubleField = isDoubleFieldResolver.apply(field);
        return new StarTreeValue(field, aggregations, isDoubleField);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StarTreeValue that = (StarTreeValue) o;
        return isDoubleField == that.isDoubleField
            && Objects.equals(field, that.field)
            && Objects.equals(aggregations, that.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, aggregations, isDoubleField);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
