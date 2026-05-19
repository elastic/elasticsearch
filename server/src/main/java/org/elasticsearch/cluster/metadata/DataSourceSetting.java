/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A validated data source setting value paired with its sensitivity classification, stored in
 * cluster state as part of data source metadata.
 *
 * <p>Secret values are either a plaintext {@code String} (when no encryption service was available
 * at PUT time) or an encrypted {@code byte[]} (the binary {@code writeTo} output of an
 * {@code EncryptedData} carrier, when encryption succeeded). Runtime type discrimination
 * ({@code value instanceof byte[]}) tells them apart at every layer — binary writeGenericValue
 * preserves the type tag, SMILE XContent preserves the byte-array embedded-object token.
 *
 * <p>Non-secret values may carry any type that round-trips through {@link StreamOutput#writeGenericValue}.
 * Access values via {@link #rawValue()} (always returns the raw value) or {@link #nonSecretValue()}
 * (asserts {@code !secret}).
 */
public final class DataSourceSetting implements Writeable, ToXContentObject {

    public static final String MASK_SENTINEL = "::es_redacted::";

    private static final ParseField VALUE = new ParseField("value");
    private static final ParseField SECRET = new ParseField("secret");

    private static final ConstructingObjectParser<DataSourceSetting, Void> PARSER = new ConstructingObjectParser<>(
        "data_source_setting",
        false,
        (args, ctx) -> new DataSourceSetting(args[0], (boolean) args[1])
    );

    static {
        // Value field accepts scalars (plaintext String, numbers, booleans, null) and binary embedded
        // objects (encrypted byte[] from SMILE persistence). The parser dispatches on token kind.
        PARSER.declareField(ConstructingObjectParser.constructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                return p.binaryValue();
            }
            return p.objectText();
        }, VALUE, ObjectParser.ValueType.VALUE_OBJECT_ARRAY);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SECRET);
    }

    private final Object value;
    private final boolean secret;

    public DataSourceSetting(Object value, boolean secret) {
        if (secret && value != null) {
            if ((value instanceof String || value instanceof byte[]) == false) {
                throw new IllegalArgumentException(
                    "secret data source settings must be a String (plaintext) or byte[] (encrypted blob); got ["
                        + value.getClass().getName()
                        + "]"
                );
            }
        }
        this.value = value;
        this.secret = secret;
    }

    public DataSourceSetting(StreamInput in) throws IOException {
        this(in.readGenericValue(), in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(value);
        out.writeBoolean(secret);
    }

    public boolean secret() {
        return secret;
    }

    /** True iff the value is an encrypted blob (byte[] — the binary writeTo of an EncryptedData). */
    public boolean isEncryptedBlob() {
        return value instanceof byte[];
    }

    /** Returns the value of a non-secret setting; throws if secret. */
    public Object nonSecretValue() {
        if (secret) {
            throw new IllegalStateException("secret setting — use rawValue() and route through the decrypt helper");
        }
        return value;
    }

    /** Raw value irrespective of secret flag. Callers must route secret values through the consumer-side decrypt helper. */
    public Object rawValue() {
        return value;
    }

    /** Returns the masked sentinel for secrets, or the plaintext value otherwise. Safe for REST responses. */
    public Object presentationValue() {
        return secret ? MASK_SENTINEL : value;
    }

    public static DataSourceSetting fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (value instanceof byte[] bytes) {
            // Renders as base64 in JSON, embedded-object token in SMILE — the SMILE form is what
            // PersistedClusterStateService round-trips through.
            builder.field(VALUE.getPreferredName(), bytes);
        } else {
            builder.field(VALUE.getPreferredName(), value);
        }
        builder.field(SECRET.getPreferredName(), secret);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSourceSetting that = (DataSourceSetting) o;
        if (secret != that.secret) return false;
        if (value instanceof byte[] lhs && that.value instanceof byte[] rhs) {
            return Arrays.equals(lhs, rhs);
        }
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int v = value instanceof byte[] bytes ? Arrays.hashCode(bytes) : Objects.hashCode(value);
        return Objects.hash(v, secret);
    }

    @Override
    public String toString() {
        return "DataSourceSetting{value=" + (secret ? MASK_SENTINEL : value) + ", secret=" + secret + "}";
    }
}
