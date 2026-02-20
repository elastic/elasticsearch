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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.validateMapStringValues;

public record Headers(@Nullable Map<String, String> headers) implements ToXContentFragment, Writeable {

    private static final ParseField HEADERS = new ParseField("headers");

    private static final ConstructingObjectParser<Headers, Void> PARSER = new ConstructingObjectParser<>(
        Headers.class.getSimpleName(),
        true,
        Headers::create
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapOrdered(), HEADERS);
    }

    @SuppressWarnings("unchecked")
    private static Headers create(Object[] args) {
        var validationException = new ValidationException();
        var stringHeaders = validateMapStringValues(
            (Map<String, String>) args[0],
            HEADERS.getPreferredName(),
            validationException,
            false,
            null
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new Headers(stringHeaders);
    }

    /**
     * Parses a {@link Headers} from the given parser. The parser must be positioned on an object that may contain
     * an optional {@code headers} field (a map of string to string).
     */
    public static Headers parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public Headers(StreamInput in) throws IOException {
        this(in.readOptionalImmutableMap(StreamInput::readString, StreamInput::readString));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (headers != null) {
            builder.field(HEADERS.getPreferredName(), headers);
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalMap(headers, StreamOutput::writeString, StreamOutput::writeString);
    }
}
