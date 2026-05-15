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
 * A validated data source setting value paired with its sensitivity classification.
 * Stored in cluster state as part of data source metadata.
 *
 * <p>Secret settings can hold one of three in-memory shapes:
 * <ul>
 *   <li>{@code String} or {@code null} — plaintext, the legacy and transient post-validator-pre-encrypt form.</li>
 *   <li>{@link GenericNamedWriteable} — a ciphertext carrier (e.g. {@code EncryptedData}) deposited by the
 *       master-side encrypt step. Travels on the wire via the byte-tag-30 dispatch in
 *       {@link StreamOutput#writeGenericValue} and {@link StreamInput#readGenericValue}.</li>
 *   <li>{@code Map<String, Object>} — the parsed-from-XContent form of an encrypted ciphertext object.
 *       Equivalent semantically to the {@link GenericNamedWriteable} form; the consumer-side decrypt
 *       helper handles either shape.</li>
 * </ul>
 * Non-secret settings may carry any value that round-trips through {@link StreamOutput#writeGenericValue}
 * and the XContent writer (in practice: String, Integer, Long, Double, Boolean, null, and nested maps or lists).
 *
 * <p>Read accessors are split by secret classification so that access to plaintext secrets is always
 * explicit at the call site. There is no generic {@code value()} accessor.
 * <ul>
 *   <li>{@link #nonSecretValue()} — returns the {@code Object} for a non-secret setting; throws if the setting is a secret.</li>
 *   <li>{@link #encryptedSecret()} — returns the raw ciphertext carrier or legacy plaintext {@code String} for a
 *       secret setting; the consumer-side decrypt helper handles the type-switch. Throws if not a secret.</li>
 *   <li>{@link #secretValue()} — legacy accessor that returns a {@link SecureString} when the secret is plaintext
 *       (pre-encryption); throws when the secret is encrypted. New code should prefer {@link #encryptedSecret()}.</li>
 *   <li>{@link #presentationValue()} — masks if secret, else returns the plaintext; safe for REST responses.</li>
 *   <li>{@link #toString()} — masks secret values.</li>
 * </ul>
 *
 * <p><b>Encryption contract.</b> The four serialization touch points — {@link #writeTo}, {@link #toXContent},
 * the {@link #DataSourceSetting(StreamInput) StreamInput constructor}, and {@link #fromXContent} — carry
 * whichever in-memory shape is currently held in {@code value}. The encryption layer wraps secrets into
 * a {@link GenericNamedWriteable} on PUT (master-side encrypt step); decrypt happens at the consumer call site.
 */
public final class DataSourceSetting implements Writeable, ToXContentObject {

    static final String MASK_SENTINEL = "::es_redacted::";

    /** Cap on encrypted-payload size when parsed from XContent — credentials are short; rejects hostile bloat early. */
    static final int MAX_ENCRYPTED_PAYLOAD_BYTES = 4096;

    private static final ParseField VALUE = new ParseField("value");
    private static final ParseField SECRET = new ParseField("secret");

    private static final ConstructingObjectParser<DataSourceSetting, Void> PARSER = new ConstructingObjectParser<>(
        "data_source_setting",
        false,
        (args, ctx) -> new DataSourceSetting(args[0], (boolean) args[1])
    );

    static {
        // Dual-format read: a scalar (legacy plaintext) OR a START_OBJECT (encrypted-data carrier as a Map).
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
            // Permissible: plaintext String, ciphertext carrier (GenericNamedWriteable like EncryptedData),
            // or parsed-from-XContent Map (the encrypted-object shape).
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
        // writeGenericValue and readGenericValue dispatch by byte tag; the EncryptedData GenericNamedWriteable
        // (tag 30) round-trips transparently. Legacy plaintext String round-trips as the String tag.
        this(in.readGenericValue(), in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // value's runtime type determines the byte tag: String, byte-tag-30 GenericNamedWriteable for
        // EncryptedData, or the Map tag for the XContent-parsed encrypted-object shape.
        out.writeGenericValue(value);
        out.writeBoolean(secret);
    }

    public boolean secret() {
        return secret;
    }

    /**
     * In-memory value for a non-secret setting. Throws if this setting is a secret — secret values must go
     * through {@link #encryptedSecret()} (or {@link #secretValue()} for the legacy plaintext path) so that
     * access to ciphertext or plaintext is explicit at every call site.
     */
    public Object nonSecretValue() {
        if (secret) {
            throw new IllegalStateException("secret setting — use encryptedSecret() or secretValue()");
        }
        return value;
    }

    /**
     * The raw secret carrier, regardless of whether it is plaintext (legacy {@code String}), a ciphertext
     * {@link GenericNamedWriteable} (e.g. {@code EncryptedData}), or a {@code Map<String, Object>} parsed from
     * encrypted XContent. The consumer-side decrypt helper inspects the type and acts accordingly.
     *
     * <p>Throws if this setting is not classified as a secret.
     */
    public Object encryptedSecret() {
        if (secret == false) {
            throw new IllegalStateException("not a secret setting — use nonSecretValue()");
        }
        return value;
    }

    /**
     * Legacy plaintext-only secret accessor returning a {@link SecureString}. Works only when the in-memory
     * value is a plaintext {@code String} (pre-encryption or never-encrypted). Throws on the encrypted carriers;
     * those callers should switch to {@link #encryptedSecret()} and run the value through the decrypt helper.
     *
     * <p>Caller should use try-with-resources to clear the chars.
     *
     * <p>Memory-hygiene caveat: the underlying {@code value} is stored as a Java {@code String}, so zeroing
     * the returned {@code SecureString} chars does not zero the originating backing string. The {@link SecureString}
     * wrapper is primarily a warning-typed API; true memory hygiene requires the consumer-side decrypt path
     * (which materializes from {@code byte[]} via {@code char[]} without an intermediate immutable {@code String}).
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
        // For ciphertext-carrier shapes (e.g. EncryptedData), delegate to ToXContentObject so the emitted
        // structure matches the carrier's canonical shape. For Map values (parsed-from-XContent), emit as
        // an object. Scalar values are emitted directly.
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
