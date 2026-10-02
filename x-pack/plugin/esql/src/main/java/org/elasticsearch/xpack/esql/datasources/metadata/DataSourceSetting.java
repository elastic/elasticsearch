/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.metadata;

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
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A data source setting value plus its secret/non-secret classification, stored in cluster state. A
 * non-secret holds its plaintext value; a secret holds an {@link EncryptedData} carrier directly (or
 * {@code null}), never raw plaintext — {@code value instanceof EncryptedData} is the discriminator.
 * Serialization delegates to {@link EncryptedData}'s own {@link Writeable}/{@link ToXContentObject}
 * rather than re-encoding the bytes.
 */
public final class DataSourceSetting implements Writeable, ToXContentObject {

    public static final String MASK_SENTINEL = "::es_redacted::";

    private static final ParseField VALUE = new ParseField("value");
    private static final ParseField ENCRYPTED_VALUE = new ParseField("encrypted_value");
    private static final ParseField SECRET = new ParseField("secret");

    private static final ConstructingObjectParser<DataSourceSetting, Void> PARSER = new ConstructingObjectParser<>(
        "data_source_setting",
        false,
        (args, ctx) -> {
            Object plain = args[0];
            EncryptedData encrypted = (EncryptedData) args[1];
            boolean secret = (boolean) args[2];
            return new DataSourceSetting(encrypted != null ? encrypted : plain, secret);
        }
    );

    static {
        // Exactly one of value / encrypted_value is present. The plaintext value accepts scalars
        // (String, numbers, booleans, null); the encrypted carrier is a nested {key_id, data} object.
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> p.objectText(),
            VALUE,
            ObjectParser.ValueType.VALUE_OBJECT_ARRAY
        );
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> EncryptedData.fromXContent(p), ENCRYPTED_VALUE);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SECRET);
    }

    private final Object value;
    private final boolean secret;

    public DataSourceSetting(Object value, boolean secret) {
        this.value = value;
        this.secret = secret;
    }

    public DataSourceSetting(StreamInput in) throws IOException {
        // Wire order (left-to-right arg evaluation): encrypted flag + value, then the secret flag.
        this(in.readBoolean() ? new EncryptedData(in) : in.readGenericValue(), in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (value instanceof EncryptedData encrypted) {
            out.writeBoolean(true);
            encrypted.writeTo(out);
        } else {
            out.writeBoolean(false);
            out.writeGenericValue(value);
        }
        out.writeBoolean(secret);
    }

    public boolean secret() {
        return secret;
    }

    /** True iff {@link #rawValue()} is an encrypted carrier rather than plaintext. */
    public boolean isEncrypted() {
        return value instanceof EncryptedData;
    }

    /** Returns the value of a non-secret setting; throws if secret. */
    public Object nonSecretValue() {
        if (secret) {
            throw new IllegalStateException("internal error: cannot read a secret data source setting as plaintext");
        }
        return value;
    }

    /** Raw stored value — an {@link EncryptedData} for an encrypted secret, otherwise the plaintext value. */
    public Object rawValue() {
        return value;
    }

    /** Returns the masked sentinel for set secrets, {@code null} for wiped secrets, or the plaintext value for non-secrets. */
    public Object presentationValue() {
        if (secret == false) return value;
        return value == null ? null : MASK_SENTINEL;
    }

    public static DataSourceSetting fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (value instanceof EncryptedData encrypted) {
            builder.field(ENCRYPTED_VALUE.getPreferredName());
            encrypted.toXContent(builder, params);
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
        if (secret != that.secret) {
            return false;
        }
        // A non-secret value can be a byte[] (writeGenericValue round-trips it); Objects.equals would compare by identity.
        if (value instanceof byte[] a && that.value instanceof byte[] b) {
            return Arrays.equals(a, b);
        }
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value instanceof byte[] b ? Arrays.hashCode(b) : value, secret);
    }

    @Override
    public String toString() {
        return "DataSourceSetting{value=" + (secret ? MASK_SENTINEL : value) + ", secret=" + secret + "}";
    }
}
