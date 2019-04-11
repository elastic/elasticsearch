/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Represents an enrich policy including its configuration.
 */
public final class EnrichPolicy implements Writeable, ToXContentObject {

    static final ParseField TYPE = new ParseField("type");
    static final ParseField UPDATE_INTERVAL = new ParseField("update_interval");
    static final ParseField SOURCE_INDEX = new ParseField("source_index");
    static final ParseField QUERY_FIELD = new ParseField("query_field");
    static final ParseField DECORATE_FIELDS = new ParseField("decorate_fields");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<EnrichPolicy, Void> PARSER = new ConstructingObjectParser<>(
        "policy",
        args -> {
            return new EnrichPolicy(Type.read((String) args[0]), (TimeValue) args[1], (String) args[2], (String) args[3],
                (List<String>) args[4]);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), UPDATE_INTERVAL.getPreferredName()),
            UPDATE_INTERVAL, ObjectParser.ValueType.STRING);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SOURCE_INDEX);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), QUERY_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), DECORATE_FIELDS);
    }

    private final Type type;
    private final TimeValue updateInterval;
    private final String sourceIndex;
    private final String queryField;
    private final List<String> decorateFields;

    public EnrichPolicy(StreamInput in) throws IOException {
        this(Type.read(in.readString()), in.readTimeValue(), in.readString(), in.readString(), in.readStringList());
    }

    public EnrichPolicy(Type type,
                        TimeValue updateInterval,
                        String sourceIndex,
                        String queryField,
                        List<String> decorateFields) {
        this.type = type;
        this.updateInterval = updateInterval;
        this.sourceIndex = sourceIndex;
        this.queryField = queryField;
        this.decorateFields = decorateFields;
    }

    public Type getType() {
        return type;
    }

    public TimeValue getUpdateInterval() {
        return updateInterval;
    }

    public String getSourceIndex() {
        return sourceIndex;
    }

    public String getQueryField() {
        return queryField;
    }

    public List<String> getDecorateFields() {
        return decorateFields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type.toString());
        out.writeTimeValue(updateInterval);
        out.writeString(sourceIndex);
        out.writeString(queryField);
        out.writeStringCollection(decorateFields);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TYPE.getPreferredName(), type.toString());
        builder.field(UPDATE_INTERVAL.getPreferredName(), updateInterval.getStringRep());
        builder.field(SOURCE_INDEX.getPreferredName(), sourceIndex);
        builder.field(QUERY_FIELD.getPreferredName(), queryField);
        builder.array(DECORATE_FIELDS.getPreferredName(), decorateFields.toArray(new String[0]));
        return builder;
    }

    @Override
    public boolean isFragment() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichPolicy policy = (EnrichPolicy) o;
        return type == policy.type &&
            updateInterval.equals(policy.updateInterval) &&
            sourceIndex.equals(policy.sourceIndex) &&
            queryField.equals(policy.queryField) &&
            decorateFields.equals(policy.decorateFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, updateInterval, sourceIndex, queryField, decorateFields);
    }

    public enum Type {

        STRING;

        public static Type read(String in) {
            switch (in) {
                case "string":
                    return STRING;
                default:
                    throw new IllegalArgumentException("unknown value [" + in + "]");
            }
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }
}
