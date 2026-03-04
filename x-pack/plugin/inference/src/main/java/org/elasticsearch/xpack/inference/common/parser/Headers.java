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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeNullValues;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.validateMapStringValues;

public record Headers(StatefulValue<Map<String, String>> mapValue) implements ToXContentFragment, Writeable {

    // public for testing
    public static final String HEADERS_FIELD = "headers";
    // public for testing
    public static final Headers UNDEFINED_INSTANCE = new Headers(StatefulValue.undefined());
    public static final Headers NULL_INSTANCE = new Headers(StatefulValue.nullInstance());

    /**
     * Sentinel passed by the parser when the headers field is present with value null.
     */
    public static final Object PARSER_NULL_SENTINEL = new HashMap<>();

    private static final ParseField HEADERS = new ParseField(HEADERS_FIELD);

    public static <Value, Context> void initParser(ConstructingObjectParser<Value, Context> parser) {
        parser.declareObjectOrNull(optionalConstructorArg(), (p, c) -> {
            var parsedMap = p.map();
            if (parsedMap == null || parsedMap == PARSER_NULL_SENTINEL) {
                return parsedMap;
            }

            var validationException = new ValidationException();

            return doValidation(parsedMap, validationException);
        }, PARSER_NULL_SENTINEL, HEADERS);
    }

    private static Map<String, String> doValidation(Map<String, Object> map, ValidationException validationException) {
        removeNullValues(map);

        var stringHeaders = validateMapStringValues(map, HEADERS.getPreferredName(), validationException, false, Map.of());

        validationException.throwIfValidationErrorsExist();

        return stringHeaders;
    }

    @SuppressWarnings("unchecked")
    public static Headers create(Object arg, String path) {
        // We will get null here if the headers field was not present in the json
        if (arg == null) {
            return UNDEFINED_INSTANCE;
        }

        if (arg == PARSER_NULL_SENTINEL) {
            return NULL_INSTANCE;
        }

        var validationException = new ValidationException();

        if (arg instanceof Map == false) {
            validationException.addValidationError(ObjectParserUtils.invalidTypeErrorMsg(HEADERS_FIELD, path, arg, "Map"));
            throw validationException;
        }

        // It's not likely that this create method would be called with invalid values since they should be validated during parsing but
        // we'll do it just in case this method is used elsewhere
        var stringsMap = doValidation((Map<String, Object>) arg, validationException);

        if (stringsMap.isEmpty()) {
            // If a user specifies "headers": {} we'll assume they don't want any headers. If this in the context of an update API,
            // this is the same as if they did "headers": null which means to remove all existing headers.
            return NULL_INSTANCE;
        }

        return new Headers(StatefulValue.of(stringsMap));
    }

    public Headers {
        Objects.requireNonNull(mapValue);
    }

    public Headers(StreamInput in) throws IOException {
        this(StatefulValue.read(in, input -> input.readImmutableMap(StreamInput::readString, StreamInput::readString)));
    }

    public boolean isEmpty() {
        return mapValue.isPresent() == false || mapValue.get().isEmpty();
    }

    public boolean isPresent() {
        return mapValue.isPresent();
    }

    public boolean isNull() {
        return mapValue.isNull();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (isEmpty() == false) {
            builder.field(HEADERS.getPreferredName(), mapValue.get());
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        StatefulValue.write(
            out,
            mapValue,
            (streamOutput, v) -> streamOutput.writeMap(v, StreamOutput::writeString, StreamOutput::writeString)
        );
    }
}
