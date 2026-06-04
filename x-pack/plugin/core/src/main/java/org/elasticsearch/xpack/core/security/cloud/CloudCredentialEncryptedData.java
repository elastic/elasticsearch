/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A minimal envelope that pairs a key identifier with an opaque ciphertext payload.
 * Used by {@link PersistedCloudCredential} to store the AES-256-GCM–encrypted credential
 * without coupling x-pack-core to the encryption SPI.
 */
public final class CloudCredentialEncryptedData implements Writeable, ToXContentObject {

    private static final ParseField KEY_ID_FIELD = new ParseField("key_id");
    private static final ParseField DATA_FIELD = new ParseField("data");

    private static final ConstructingObjectParser<CloudCredentialEncryptedData, Void> PARSER = new ConstructingObjectParser<>(
        "cloud_credential_encrypted_data",
        true,
        args -> {
            String keyId = (String) args[0];
            byte[] payload = Base64.getDecoder().decode((String) args[1]);
            return new CloudCredentialEncryptedData(keyId, payload);
        }
    );

    static {
        PARSER.declareString(constructorArg(), KEY_ID_FIELD);
        PARSER.declareString(constructorArg(), DATA_FIELD);
    }

    private final String keyId;
    private final byte[] payload;

    public CloudCredentialEncryptedData(String keyId, byte[] payload) {
        this.keyId = Objects.requireNonNull(keyId, "keyId must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        this.payload = payload.clone();
    }

    public CloudCredentialEncryptedData(StreamInput in) throws IOException {
        this.keyId = in.readString();
        this.payload = in.readByteArray();
    }

    public String keyId() {
        return keyId;
    }

    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(keyId);
        out.writeByteArray(payload);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(KEY_ID_FIELD.getPreferredName(), keyId);
        builder.field(DATA_FIELD.getPreferredName(), Base64.getEncoder().encodeToString(payload));
        return builder.endObject();
    }

    public static CloudCredentialEncryptedData fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof CloudCredentialEncryptedData other) {
            return keyId.equals(other.keyId) && Arrays.equals(payload, other.payload);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 31 * keyId.hashCode() + Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
        return "CloudCredentialEncryptedData{keyId=" + keyId + ", payloadLength=" + payload.length + "}";
    }
}
