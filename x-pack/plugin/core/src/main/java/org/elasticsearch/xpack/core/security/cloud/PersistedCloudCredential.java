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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Persistence envelope for a cloud-managed credential. v2 stores AES-256-GCM ciphertext
 * ({@link CloudCredentialEncryptedData}) instead of the plaintext API key.
 * The at-rest XContent format is always v2; v1 documents are rejected at parse time.
 */
public final class PersistedCloudCredential implements Writeable, ToXContentObject {

    public static final int CURRENT_VERSION = 2;

    /**
     * Guards the v2 wire format. Peers that do not yet support this version cannot receive
     * a v2 credential on the wire; publishing to such peers will throw {@link IllegalStateException}.
     */
    public static final TransportVersion CLOUD_CREDENTIAL_ENCRYPTION = TransportVersion.fromName("cloud_credential_encryption");

    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField ENCRYPTED_FIELD = new ParseField("encrypted");

    private static final ConstructingObjectParser<PersistedCloudCredential, Void> PARSER = new ConstructingObjectParser<>(
        "persisted_cloud_credential",
        true,
        args -> {
            int version = (int) args[0];
            if (version != CURRENT_VERSION) {
                throw new IllegalStateException(
                    "unsupported at-rest version [" + version + "]; only [" + CURRENT_VERSION + "] is supported"
                );
            }
            CloudCredentialEncryptedData encrypted = (CloudCredentialEncryptedData) args[2];
            if (encrypted == null) {
                throw new IllegalStateException("encrypted field is required");
            }
            return new PersistedCloudCredential((String) args[1], encrypted);
        }
    );

    static {
        PARSER.declareInt(constructorArg(), VERSION_FIELD);
        PARSER.declareString(constructorArg(), ID_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> CloudCredentialEncryptedData.fromXContent(p), ENCRYPTED_FIELD);
    }

    private final int version;
    private final String id;
    private final CloudCredentialEncryptedData encrypted;

    public PersistedCloudCredential(String id, CloudCredentialEncryptedData encrypted) {
        this.version = CURRENT_VERSION;
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.encrypted = Objects.requireNonNull(encrypted, "encrypted must not be null");
    }

    public PersistedCloudCredential(StreamInput in) throws IOException {
        int wireVersion = in.readVInt();
        this.id = in.readString();
        this.encrypted = switch (wireVersion) {
            case 2 -> new CloudCredentialEncryptedData(in);
            default -> throw new IllegalStateException("unsupported wire version [" + wireVersion + "]");
        };
        this.version = CURRENT_VERSION;
    }

    public int version() {
        return version;
    }

    public String id() {
        return id;
    }

    public CloudCredentialEncryptedData encrypted() {
        return encrypted;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(CLOUD_CREDENTIAL_ENCRYPTION) == false) {
            throw new IllegalStateException(
                "cannot serialize to a peer that does not support transport version ["
                    + CLOUD_CREDENTIAL_ENCRYPTION
                    + "]; ensure all nodes are upgraded before publishing cloud credentials"
            );
        }
        out.writeVInt(2);
        out.writeString(id);
        encrypted.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VERSION_FIELD.getPreferredName(), version);
        builder.field(ID_FIELD.getPreferredName(), id);
        builder.field(ENCRYPTED_FIELD.getPreferredName(), encrypted);
        return builder.endObject();
    }

    public static PersistedCloudCredential fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof PersistedCloudCredential other) {
            return version == other.version && id.equals(other.id) && encrypted.equals(other.encrypted);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, id, encrypted);
    }

    @Override
    public String toString() {
        return "PersistedCloudCredential{version=" + version + ", id=" + id + ", keyId=" + encrypted.keyId() + "}";
    }
}
