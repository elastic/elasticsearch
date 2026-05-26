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
import java.util.Locale;
import java.util.Objects;

/**
 * A validated data source setting value paired with its sensitivity classification, stored in
 * cluster state as part of data source metadata.
 *
 * <p>Whether a secret is encrypted is an explicit, serialized property — {@link #encryption} — not
 * an inference from the value's runtime type. {@link EncryptionFormat#NONE} means the value is
 * plaintext as supplied by the validator; {@link EncryptionFormat#V1} means the value is the binary
 * {@code writeTo} output of an {@code EncryptedData} carrier. Both serialization paths dispatch on
 * the enum: the binary wire reads/writes the V1 value via {@code readByteArray}/{@code writeByteArray}
 * and the NONE value via {@code readGenericValue}/{@code writeGenericValue}; the XContent path
 * writes V1 as bytes (rendered as base64 in JSON, embedded-object token in SMILE) and NONE as a
 * generic value. An absent {@code encryption} field parses as {@code NONE}, so anything written
 * before this field existed reads back as plaintext (which it is).
 *
 * <p>The value's Java type is no longer a discriminator, so plaintext values carry whatever type the
 * validator produced. The only structural invariant is that a {@code V1} value is the ciphertext
 * {@code byte[]} — the consumer-side decryption step reads it as such.
 *
 * <p>Access values via {@link #rawValue()} (always returns the raw value) or {@link #nonSecretValue()}
 * (asserts {@code !secret}).
 */
public final class DataSourceSetting implements Writeable, ToXContentObject {

    public static final String MASK_SENTINEL = "::es_redacted::";

    /** Whether {@link #rawValue()} is plaintext ({@code NONE}) or an encrypted carrier blob ({@code V1}). */
    public enum EncryptionFormat {
        // DO NOT REORDER — writeTo serializes by ordinal; reordering breaks the cluster-state wire format.
        // DataSourceSettingTests#testEncryptionFormatWireOrdinals pins this; any new constant must be appended.
        NONE,
        V1;

        static EncryptionFormat fromString(String s) {
            if (s == null) {
                return NONE;
            }
            try {
                return valueOf(s.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("unknown data source encryption format [" + s + "]", e);
            }
        }

        String wireName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private static final ParseField VALUE = new ParseField("value");
    private static final ParseField SECRET = new ParseField("secret");
    private static final ParseField ENCRYPTION = new ParseField("encryption");

    private static final ConstructingObjectParser<DataSourceSetting, Void> PARSER = new ConstructingObjectParser<>(
        "data_source_setting",
        false,
        (args, ctx) -> new DataSourceSetting(args[0], (boolean) args[1], EncryptionFormat.fromString((String) args[2]))
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
        // Optional: absent => NONE (anything persisted before this field existed is plaintext).
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ENCRYPTION);
    }

    private final Object value;
    private final boolean secret;
    private final EncryptionFormat encryption;

    public DataSourceSetting(Object value, boolean secret) {
        this(value, secret, EncryptionFormat.NONE);
    }

    public DataSourceSetting(Object value, boolean secret, EncryptionFormat encryption) {
        this.encryption = Objects.requireNonNull(encryption, "encryption");
        if (encryption == EncryptionFormat.V1 && (value instanceof byte[]) == false) {
            // Not a constraint on what a secret may be — a V1 value *is* the ciphertext, which is
            // bytes by construction. Failing here beats a ClassCastException deep in the decryption step.
            throw new IllegalArgumentException(
                "an encrypted data source setting value must be a byte[] ciphertext blob; got ["
                    + (value == null ? "null" : value.getClass().getName())
                    + "]"
            );
        }
        this.value = value;
        this.secret = secret;
    }

    public DataSourceSetting(StreamInput in) throws IOException {
        // Stream order: secret, encryption, value. Value is read by dispatching on the encryption
        // discriminator (V1 → raw byte[] ciphertext, NONE → generic plaintext value) so the wire
        // shape commits to the invariant we already enforce in the constructor.
        this(in, in.readBoolean(), in.readEnum(EncryptionFormat.class));
    }

    private DataSourceSetting(StreamInput in, boolean secret, EncryptionFormat encryption) throws IOException {
        this(encryption == EncryptionFormat.V1 ? in.readByteArray() : in.readGenericValue(), secret, encryption);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(secret);
        out.writeEnum(encryption);
        if (encryption == EncryptionFormat.V1) {
            out.writeByteArray((byte[]) value);
        } else {
            out.writeGenericValue(value);
        }
    }

    public boolean secret() {
        return secret;
    }

    public EncryptionFormat encryption() {
        return encryption;
    }

    /** True iff {@link #rawValue()} is an encrypted carrier blob rather than plaintext. */
    public boolean isEncryptedBlob() {
        return encryption != EncryptionFormat.NONE;
    }

    /** Returns the value of a non-secret setting; throws if secret. */
    public Object nonSecretValue() {
        if (secret) {
            throw new IllegalStateException("internal error: cannot read a secret data source setting as plaintext");
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
        if (encryption == EncryptionFormat.V1) {
            // V1 invariant: value is byte[] ciphertext. Renders as base64 in JSON, embedded-object
            // token in SMILE — the SMILE form is what PersistedClusterStateService round-trips through.
            builder.field(VALUE.getPreferredName(), (byte[]) value);
        } else {
            builder.field(VALUE.getPreferredName(), value);
        }
        builder.field(SECRET.getPreferredName(), secret);
        builder.field(ENCRYPTION.getPreferredName(), encryption.wireName());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSourceSetting that = (DataSourceSetting) o;
        if (secret != that.secret) return false;
        if (encryption != that.encryption) return false;
        if (value instanceof byte[] lhs && that.value instanceof byte[] rhs) {
            return Arrays.equals(lhs, rhs);
        }
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        // byte[].hashCode() is identity-based — use Arrays.hashCode for content equality.
        int valueHash = value instanceof byte[] bytes ? Arrays.hashCode(bytes) : Objects.hashCode(value);
        return Objects.hash(valueHash, secret, encryption);
    }

    @Override
    public String toString() {
        return "DataSourceSetting{value=" + (secret ? MASK_SENTINEL : value) + ", secret=" + secret + ", encryption=" + encryption + "}";
    }
}
