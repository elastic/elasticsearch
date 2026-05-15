/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk.serializer;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Wire representation of a single OTel {@link io.opentelemetry.api.common.Attributes} entry.
 */
record AttributeRecord(String key, AttributeType type, Object value) implements ToXContentObject {

    private static final ParseField KEY = new ParseField("key");
    private static final ParseField TYPE = new ParseField("type");
    private static final ParseField VALUE = new ParseField("value");

    static final ConstructingObjectParser<AttributeRecord, Void> PARSER = new ConstructingObjectParser<>(
        "attribute",
        true,
        args -> new AttributeRecord((String) args[0], AttributeType.valueOf((String) args[1]), args[2])
    );
    static {
        PARSER.declareString(constructorArg(), KEY);
        PARSER.declareString(constructorArg(), TYPE);
        PARSER.declareField(constructorArg(), XContentParserUtils::parseFieldsValue, VALUE, ObjectParser.ValueType.VALUE_ARRAY);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field(KEY.getPreferredName(), key);
        b.field(TYPE.getPreferredName(), type.name());
        b.field(VALUE.getPreferredName(), value);
        b.endObject();
        return b;
    }

    /**
     * Applies this entry to an {@link AttributesBuilder}, narrowing {@link #value} to the Java type implied by {@link #type}.
     */
    void applyTo(AttributesBuilder builder) {
        switch (type) {
            case STRING -> builder.put(AttributeKey.stringKey(key), (String) value);
            case BOOLEAN -> builder.put(AttributeKey.booleanKey(key), (Boolean) value);
            case LONG -> builder.put(AttributeKey.longKey(key), ((Number) value).longValue());
            case DOUBLE -> builder.put(AttributeKey.doubleKey(key), ((Number) value).doubleValue());
            case STRING_ARRAY -> builder.put(AttributeKey.stringArrayKey(key), this.<String>asList());
            case BOOLEAN_ARRAY -> builder.put(AttributeKey.booleanArrayKey(key), this.<Boolean>asList());
            case LONG_ARRAY -> builder.put(AttributeKey.longArrayKey(key), this.<Number>asList().stream().map(Number::longValue).toList());
            case DOUBLE_ARRAY -> builder.put(
                AttributeKey.doubleArrayKey(key),
                this.<Number>asList().stream().map(Number::doubleValue).toList()
            );
        }
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> asList() {
        return (List<T>) value;
    }

    static List<AttributeRecord> fromAttributes(Attributes attrs) {
        if (attrs.isEmpty()) {
            return List.of();
        }
        List<AttributeRecord> entries = new ArrayList<>(attrs.size());
        attrs.forEach((key, value) -> entries.add(new AttributeRecord(key.getKey(), key.getType(), value)));
        return entries;
    }

    static Attributes toAttributes(List<AttributeRecord> entries) {
        if (entries.isEmpty()) {
            return Attributes.empty();
        }
        AttributesBuilder builder = Attributes.builder();
        for (AttributeRecord entry : entries) {
            entry.applyTo(builder);
        }
        return builder.build();
    }

    static void writeArray(XContentBuilder builder, String fieldName, List<AttributeRecord> entries, ToXContent.Params params)
        throws IOException {
        builder.startArray(fieldName);
        for (AttributeRecord entry : entries) {
            entry.toXContent(builder, params);
        }
        builder.endArray();
    }
}
