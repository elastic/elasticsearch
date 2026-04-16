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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A validated data source setting value paired with its sensitivity classification.
 * Stored in cluster state as part of data source metadata.
 *
 * <p>Secret settings must be String-valued (or null) — the constructor enforces this invariant. Non-secret settings
 * may carry any value that round-trips through {@link StreamOutput#writeGenericValue} and the XContent writer
 * (in practice: String, Integer, Long, Double, Boolean, null, and nested maps or lists).
 *
 * <p>Read accessors are split by secret classification so that access to plaintext secrets is always
 * explicit at the call site. There is no generic {@code value()} accessor.
 * <ul>
 *   <li>{@link #nonSecretValue()} — returns the plaintext {@code Object} for a non-secret setting;
 *       throws if the setting is a secret.</li>
 *   <li>{@link #secretValue()} — returns the plaintext as a {@link SecureString} for secrets only;
 *       use in a try-with-resources to zero the chars when done. Throws if the setting is not a secret.</li>
 *   <li>{@link #presentationValue()} — masks if secret, else returns the plaintext; safe for REST responses.</li>
 *   <li>{@link #toString()} — masks secret values.</li>
 * </ul>
 *
 * <p><b>Encryption contract.</b> The {@code value} field is always plaintext in memory. The four
 * touch points marked {@code // TODO(encryption)} below — {@link #writeTo}, {@link #toXContent},
 * the {@link #DataSourceSetting(StreamInput) StreamInput constructor}, and {@link #fromXContent} —
 * are where the encryption layer wires in. Read accessors remain pure in-memory plaintext readers.
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
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> p.objectText(),
            VALUE,
            org.elasticsearch.xcontent.ObjectParser.ValueType.VALUE
        );
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SECRET);
    }

    private final Object value;
    private final boolean secret;

    public DataSourceSetting(Object value, boolean secret) {
        if (secret && (value == null || value instanceof String) == false) {
            throw new IllegalArgumentException(
                "secret data source settings must be String-valued; got [" + value.getClass().getName() + "]"
            );
        }
        this.value = value;
        this.secret = secret;
    }

    public DataSourceSetting(StreamInput in) throws IOException {
        // Delegate to the primary constructor so the same validation (secret values must be String or null) runs on
        // the wire deserialization path as on direct construction.
        this(in.readGenericValue(), in.readBoolean());
        // TODO(encryption): decrypt secret values on read.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO(encryption): encrypt secret values on write.
        out.writeGenericValue(value);
        out.writeBoolean(secret);
    }

    public boolean secret() {
        return secret;
    }

    /**
     * Plaintext in-memory value for a non-secret setting. Throws if this setting is a secret —
     * secret values must go through {@link #secretValue()} so that plaintext access is explicit at every call site.
     */
    public Object nonSecretValue() {
        if (secret) {
            throw new IllegalStateException("secret setting — use secretValue() for SecureString access");
        }
        return value;
    }

    /**
     * Plaintext secret value as a {@link SecureString}. Caller should use try-with-resources to clear the chars.
     * Throws if this setting is not classified as a secret.
     *
     * <p>Memory-hygiene caveat: the underlying {@code value} is stored as a Java {@code String} today, so zeroing
     * the returned {@code SecureString} chars does not zero the originating backing string (Java strings are
     * immutable and cannot be cleared). The {@link SecureString} wrapper is primarily a warning-typed API; true
     * memory hygiene requires end-to-end {@code char[]} storage, which lands with the encryption layer.
     */
    public SecureString secretValue() {
        if (secret == false) {
            throw new IllegalStateException("not a secret setting — use nonSecretValue()");
        }
        return value == null ? null : new SecureString(((String) value).toCharArray());
    }

    /** Returns the masked sentinel for secrets, or the plaintext value otherwise. Safe for REST responses. */
    public Object presentationValue() {
        return secret ? MASK_SENTINEL : value;
    }

    public static DataSourceSetting fromXContent(XContentParser parser) throws IOException {
        // TODO(encryption): parse and decrypt the encrypted representation for secret values.
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // TODO(encryption): emit the encrypted representation for secret values.
        builder.startObject();
        builder.field(VALUE.getPreferredName(), value);
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
