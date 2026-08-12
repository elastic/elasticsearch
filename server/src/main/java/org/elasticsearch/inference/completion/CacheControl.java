/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CACHE_CONTROL_TTL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CACHE_CONTROL_TYPE_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class represents the cache configuration for a chat completion request.
 */
// TODO: implement Accountable
public final class CacheControl implements ToXContentObject, Writeable {

    /**
     * Strict parser that rejects unknown fields. Use this for user-facing request parsing (e.g. the
     * unified completion request body and chat completion task settings) where an unrecognized field
     * indicates a client mistake that should be surfaced as an error.
     */
    public static final ConstructingObjectParser<CacheControl, Void> PARSER = createParser(false);

    /**
     * Lenient parser that ignores unknown fields. Only use this where unknown fields are acceptable,
     * such as parsing a server-to-server payload (e.g. the EIS authorization response) that may be
     * produced by a newer version of the sender. Do not use this for user-facing request parsing;
     * use {@link #PARSER} instead so client mistakes are surfaced as errors.
     */
    public static final ConstructingObjectParser<CacheControl, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<CacheControl, Void> createParser(boolean ignoreUnknownFields) {
        var parser = new ConstructingObjectParser<CacheControl, Void>(CacheControl.class.getSimpleName(), ignoreUnknownFields, args -> {
            final var type = args[0] == null ? null : (String) args[0];
            final var ttl = args[1] == null ? null : (TimeValue) args[1];

            return new CacheControl(type, ttl);
        });

        parser.declareString(optionalConstructorArg(), new ParseField(CACHE_CONTROL_TYPE_FIELD));
        parser.declareField(
            optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), null, CACHE_CONTROL_TTL_FIELD),
            new ParseField(CACHE_CONTROL_TTL_FIELD),
            ObjectParser.ValueType.STRING_OR_NULL
        );

        return parser;
    }

    @Nullable
    private final String type;

    @Nullable
    private final TimeValue ttl;

    public CacheControl(@Nullable String type, @Nullable TimeValue ttl) {
        this.type = type;
        this.ttl = ttl;
    }

    public CacheControl(StreamInput in) throws IOException {
        this.type = in.readOptionalString();
        this.ttl = in.readOptionalTimeValue();
    }

    @Nullable
    public String type() {
        return type;
    }

    @Nullable
    public TimeValue ttl() {
        return ttl;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(type);
        out.writeOptionalTimeValue(ttl);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (type != null) {
            builder.field(CACHE_CONTROL_TYPE_FIELD, type);
        }

        if (ttl != null) {
            builder.field(CACHE_CONTROL_TTL_FIELD, ttl.getStringRep());
        }

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (CacheControl) obj;
        return Objects.equals(type, that.type) && Objects.equals(ttl, that.ttl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, ttl);
    }
}
