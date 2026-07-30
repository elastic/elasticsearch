/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Persistence envelope for a cloud-managed credential, pairing the public API key {@code id} with the internal cloud API key credentials.
 *
 * <p>Two at-rest forms are supported, discriminated by {@code version}:
 * <ul>
 *   <li><b>v1 (plaintext)</b> - the internal API key is stored as-is in the {@code value} field. This form is only ever read, never
 *       written: it is the format of credentials persisted before encryption at rest was introduced.</li>
 *   <li><b>v2 (encrypted)</b> - the internal API key is stored as AES-256-GCM ciphertext in {@code encrypted}
 *       ({@link CloudCredentialEncryptedData}) under the per-project cloud credential encryption key. All new credentials use this
 *       form.</li>
 * </ul>
 */
public final class PersistedCloudCredential implements Writeable, ToXContentObject, Releasable {

    /** Plaintext at-rest form: {@code value} holds the internal API key. */
    public static final int PLAINTEXT_VERSION = 1;

    /** Encrypted at-rest form: {@code encrypted} holds the ciphertext. */
    public static final int ENCRYPTED_VERSION = 2;

    /** The version stamped on newly created credentials (always encrypted). */
    public static final int CURRENT_VERSION = ENCRYPTED_VERSION;

    /**
     * Guards the encrypted (v2) wire format.
     */
    public static final TransportVersion CLOUD_CREDENTIAL_ENCRYPTION = TransportVersion.fromName("cloud_credential_encryption");

    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField ID_FIELD = new ParseField("id");

    /**
     * The at-rest forms, discriminated by the persisted {@code version} int. Single source of truth for each form's version number and
     * payload field, so parsing, validation, and error messages carry no per-version literals.
     */
    private enum Format {

        PLAINTEXT(PLAINTEXT_VERSION, new ParseField("value")),

        ENCRYPTED(ENCRYPTED_VERSION, new ParseField("encrypted"));

        private final int version;
        private final ParseField field;

        Format(int version, ParseField field) {
            this.version = version;
            this.field = field;
        }

        private static final Supplier<String> SUPPORTED_VERSIONS = CachedSupplier.wrap(
            () -> Arrays.stream(values()).map(f -> f.version).toList().toString()
        );

        static Format fromVersion(int version) {
            for (Format format : values()) {
                if (format.version == version) {
                    return format;
                }
            }
            throw new IllegalStateException(
                "unsupported at-rest version [" + version + "], supported versions are " + SUPPORTED_VERSIONS.get()
            );
        }

        IllegalStateException requiresPayloadField() {
            return new IllegalStateException("at-rest version [" + version + "] requires the [" + field.getPreferredName() + "] field");
        }
    }

    private static final ConstructingObjectParser<PersistedCloudCredential, Void> PARSER = new ConstructingObjectParser<>(
        "persisted_cloud_credential",
        true,
        args -> {
            int version = (int) args[0];
            String id = (String) args[1];
            SecureString value = (SecureString) args[2];
            CloudCredentialEncryptedData encrypted = (CloudCredentialEncryptedData) args[3];
            return switch (Format.fromVersion(version)) {
                case PLAINTEXT -> {
                    if (value == null) {
                        throw Format.PLAINTEXT.requiresPayloadField();
                    }
                    yield PersistedCloudCredential.plaintext(id, value);
                }
                case ENCRYPTED -> {
                    if (encrypted == null) {
                        throw Format.ENCRYPTED.requiresPayloadField();
                    }
                    yield new PersistedCloudCredential(id, encrypted);
                }
            };
        }
    );

    static {
        PARSER.declareInt(constructorArg(), VERSION_FIELD);
        PARSER.declareString(constructorArg(), ID_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> new SecureString(p.text().toCharArray()),
            Format.PLAINTEXT.field,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> CloudCredentialEncryptedData.fromXContent(p), Format.ENCRYPTED.field);
    }

    private final Format format;
    private final String id;
    @Nullable
    private final SecureString internalApiKey;
    @Nullable
    private final CloudCredentialEncryptedData encrypted;

    private PersistedCloudCredential(
        Format format,
        String id,
        @Nullable SecureString internalApiKey,
        @Nullable CloudCredentialEncryptedData encrypted
    ) {
        this.format = format;
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.internalApiKey = internalApiKey;
        this.encrypted = encrypted;
        assert switch (format) {
            case PLAINTEXT -> internalApiKey != null && encrypted == null;
            case ENCRYPTED -> encrypted != null && internalApiKey == null;
        } : "payload does not match at-rest format [" + format + "]";
    }

    /**
     * Creates an encrypted (v2) credential.
     */
    public PersistedCloudCredential(String id, CloudCredentialEncryptedData encrypted) {
        this(Format.ENCRYPTED, id, null, Objects.requireNonNull(encrypted, "encrypted must not be null"));
    }

    /**
     * Creates a plaintext (v1) credential. Only used for the {@code cluster.state.encryption.required=false} opt-out and for reading
     * legacy documents.
     */
    public static PersistedCloudCredential plaintext(String id, SecureString internalApiKey) {
        return new PersistedCloudCredential(
            Format.PLAINTEXT,
            id,
            Objects.requireNonNull(internalApiKey, "internalApiKey must not be null"),
            null
        );
    }

    /**
     * Creates an encrypted (v2) credential. Equivalent to {@link #PersistedCloudCredential(String, CloudCredentialEncryptedData)}.
     */
    public static PersistedCloudCredential encrypted(String id, CloudCredentialEncryptedData encrypted) {
        return new PersistedCloudCredential(id, encrypted);
    }

    public PersistedCloudCredential(StreamInput in) throws IOException {
        this.format = Format.fromVersion(in.readVInt());
        this.id = in.readString();
        SecureString plaintext = null;
        CloudCredentialEncryptedData ciphertext = null;
        switch (format) {
            case PLAINTEXT -> plaintext = in.readSecureString();
            case ENCRYPTED -> ciphertext = new CloudCredentialEncryptedData(in);
        }
        this.internalApiKey = plaintext;
        this.encrypted = ciphertext;
    }

    public int version() {
        return format.version;
    }

    public String id() {
        return id;
    }

    /**
     * The plaintext internal API key, or {@code null} when this credential is stored encrypted.
     */
    @Nullable
    public SecureString internalApiKey() {
        return internalApiKey;
    }

    /**
     * The encrypted internal API key, or {@code null} when this credential is stored in plaintext.
     */
    @Nullable
    public CloudCredentialEncryptedData encrypted() {
        return encrypted;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(format.version);
        out.writeString(id);
        switch (format) {
            case PLAINTEXT -> out.writeSecureString(internalApiKey);
            case ENCRYPTED -> {
                if (out.getTransportVersion().supports(CLOUD_CREDENTIAL_ENCRYPTION) == false) {
                    throw new IllegalStateException(
                        "cannot write encrypted cloud credential to a peer at transport version ["
                            + out.getTransportVersion().toReleaseVersion()
                            + "]: requires ["
                            + CLOUD_CREDENTIAL_ENCRYPTION.toReleaseVersion()
                            + "] or later"
                    );
                }
                encrypted.writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VERSION_FIELD.getPreferredName(), format.version);
        builder.field(ID_FIELD.getPreferredName(), id);
        switch (format) {
            case PLAINTEXT -> builder.field(format.field.getPreferredName(), internalApiKey.toString());
            case ENCRYPTED -> builder.field(format.field.getPreferredName(), encrypted);
        }
        return builder.endObject();
    }

    public static PersistedCloudCredential fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Releases the underlying {@link SecureString}.
     */
    @Override
    public void close() {
        if (internalApiKey != null) {
            internalApiKey.close();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof PersistedCloudCredential other) {
            return format == other.format
                && id.equals(other.id)
                && Objects.equals(internalApiKey, other.internalApiKey)
                && Objects.equals(encrypted, other.encrypted);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, id, internalApiKey, encrypted);
    }

    @Override
    public String toString() {
        return switch (format) {
            case PLAINTEXT -> "PersistedCloudCredential{version="
                + format.version
                + ", apiKeyId="
                + id
                + ", internalApiKey=::es_redacted::}";
            case ENCRYPTED -> "PersistedCloudCredential{version="
                + format.version
                + ", apiKeyId="
                + id
                + ", encryptionKeyId="
                + encrypted.keyId()
                + "}";
        };
    }
}
