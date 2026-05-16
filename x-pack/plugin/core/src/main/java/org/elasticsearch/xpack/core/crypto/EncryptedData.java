/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.crypto;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

/**
 * Holds the result of an encryption operation: the key ID and the encrypted payload.
 * Implements {@link GenericNamedWriteable} so it can travel inside {@code Map<String, Object>}
 * carriers serialized via {@link StreamOutput#writeGenericValue}.
 */
public final class EncryptedData implements GenericNamedWriteable, ToXContentObject {

    /** Stable transport-name for {@link GenericNamedWriteable} dispatch; renaming is a wire-format break. */
    public static final String NAMED_WRITEABLE_NAME = "encrypted_data";

    public static final TransportVersion GENERIC_NAMED_WRITEABLE_V1 = TransportVersion.fromName("data_source_encryption");

    private final String keyId;
    private final byte[] payload;

    public EncryptedData(String keyId, byte[] payload) {
        this.keyId = Objects.requireNonNull(keyId, "keyId must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        this.payload = payload.clone();
    }

    public EncryptedData(StreamInput in) throws IOException {
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

    /** Renders as a base64 scalar wrapping the binary {@link #writeTo} output — matches the
     *  {@code License.signature} / {@code PrimaryEncryptionKeyMetadata} pattern for binary cluster-state
     *  material persisted via XContent. */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        writeTo(out);
        builder.value(Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes())));
        return builder;
    }

    public static EncryptedData fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        byte[] decoded = Base64.getDecoder().decode(parser.text());
        try (StreamInput in = new BytesArray(decoded).streamInput()) {
            return new EncryptedData(in);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EncryptedData that = (EncryptedData) o;
        return keyId.equals(that.keyId) && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyId, Arrays.hashCode(payload));
    }

    @Override
    public String getWriteableName() {
        return NAMED_WRITEABLE_NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return GENERIC_NAMED_WRITEABLE_V1;
    }

    @Override
    public String toString() {
        return "EncryptedData{keyId=" + keyId + ", payload=::es_redacted::}";
    }
}
