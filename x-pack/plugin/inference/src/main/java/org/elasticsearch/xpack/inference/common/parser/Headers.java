/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeNullValues;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.validateMapStringValues;

public record Headers(Map<String, String> headersMap) implements ToXContentFragment, Writeable {

    private static final ParseField HEADERS = new ParseField("headers");

    public static final Headers EMPTY_INSTANCE = new Headers(Map.of());

    public static <Value, Context> void initParser(ConstructingObjectParser<Value, Context> parser) {
        parser.declareObjectOrNull(optionalConstructorArg(), (p, c) -> p.mapOrdered(), null, HEADERS);
    }

    @SuppressWarnings("unchecked")
    public static Headers create(Object arg) {
        if (arg == null) {
            return null;
        }

        var validationException = new ValidationException();

        removeNullValues((Map<String, Object>) arg);

        var stringHeaders = validateMapStringValues(
            (Map<String, String>) arg,
            HEADERS.getPreferredName(),
            validationException,
            false,
            Map.of()
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        if (stringHeaders.isEmpty()) {
            return EMPTY_INSTANCE;
        }

        return new Headers(stringHeaders);
    }

    public Headers {
        Objects.requireNonNull(headersMap, "headers map is required");
    }

    public Headers(StreamInput in) throws IOException {
        this(in.readImmutableMap(StreamInput::readString, StreamInput::readString));
    }

    public boolean isEmpty() {
        return headersMap.isEmpty();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(HEADERS.getPreferredName(), headersMap);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(headersMap, StreamOutput::writeString, StreamOutput::writeString);
    }
}
