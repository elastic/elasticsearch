/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A validated data source setting value paired with its sensitivity classification, stored in
 * cluster state as part of data source metadata.
 *
 * <p>Secret values may be a plaintext {@code String} (legacy / pre-encrypt), a ciphertext
 * {@link GenericNamedWriteable} carrier ({@code EncryptedData}) deposited by the master-side
 * encrypt step, or a {@code Map<String, Object>} parsed from encrypted XContent. Non-secret values
 * may carry any type that round-trips through {@link StreamOutput#writeGenericValue}. Access plaintext
 * secrets via {@link #encryptedSecret()} (returns the raw carrier; consumer decrypts) or the legacy
 * {@link #secretValue()} (throws on encrypted carriers).
 */
public final class DataSourceSetting implements Writeable, ToXContentObject {

    static final String MASK_SENTINEL = "::es_redacted::";

    private static final ParseField VALUE = new ParseField("value");
    private static final ParseField SECRET = new ParseField("secret");

    private static final ConstructingObjectParser<DataSourceSetting, Void> PARSER = new ConstructingObjectParser<>(
        "data_source_setting",
        false,
        (args, ctx) -> new DataSourceSetting(args[0], (boolean) args[1])
    );

    static {
        // Accept either a scalar (legacy plaintext) or START_OBJECT (encrypted-data as a map).
        PARSER.declareField(ConstructingObjectParser.constructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                return p.map();
            }
            return p.objectText();
        }, VALUE, ObjectParser.ValueType.VALUE_OBJECT_ARRAY);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SECRET);
    }

    private final Object value;
    private final boolean secret;

    public DataSourceSetting(Object value, boolean secret) {
        if (secret && value != null) {
            if ((value instanceof String || value instanceof GenericNamedWriteable || value instanceof Map<?, ?>) == false) {
                throw new IllegalArgumentException(
                    "secret data source settings must be String, an encrypted carrier, or a parsed map; got ["
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

    /** Returns the value of a non-secret setting; throws if secret. */
    public Object nonSecretValue() {
        if (secret) {
            throw new IllegalStateException("secret setting — use encryptedSecret() or secretValue()");
        }
        return value;
    }

    /** Returns the raw secret carrier (String, GenericNamedWriteable, or Map); throws if not secret. */
    public Object encryptedSecret() {
        if (secret == false) {
            throw new IllegalStateException("not a secret setting — use nonSecretValue()");
        }
        return value;
    }

    /**
     * Legacy plaintext-only secret accessor returning a {@link SecureString}. Throws on encrypted
     * carriers — callers should switch to {@link #encryptedSecret()} and run the value through the
     * consumer-side decrypt helper.
     */
    public SecureString secretValue() {
        if (secret == false) {
            throw new IllegalStateException("not a secret setting — use nonSecretValue()");
        }
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return new SecureString(s.toCharArray());
        }
        throw new IllegalStateException(
            "secret value is encrypted [" + value.getClass().getName() + "] — call encryptedSecret() and decrypt"
        );
    }

    /** Returns the masked sentinel for secrets, or the plaintext value otherwise. Safe for REST responses. */
    public Object presentationValue() {
        return secret ? MASK_SENTINEL : value;
    }

    public static DataSourceSetting fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (value instanceof ToXContentObject toX) {
            builder.field(VALUE.getPreferredName());
            toX.toXContent(builder, params);
        } else if (value instanceof Map<?, ?> map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> typed = (Map<String, Object>) map;
            builder.field(VALUE.getPreferredName(), typed);
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
        return secret == that.secret && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, secret);
    }

    @Override
    public String toString() {
        return "DataSourceSetting{value=" + (secret ? MASK_SENTINEL : value) + ", secret=" + secret + "}";
    }
}
