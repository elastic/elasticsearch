/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.crypto;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Holds the result of an encryption operation: the key ID that was used and the encrypted payload.
 */
public final class EncryptedData implements Writeable, ToXContentObject {

    private static final ParseField KEY_ID_FIELD = new ParseField("key_id");
    private static final ParseField DATA_FIELD = new ParseField("data");

    private static final ConstructingObjectParser<EncryptedData, Void> PARSER = new ConstructingObjectParser<>(
        "encrypted_data",
        false,
        args -> new EncryptedData((String) args[0], (byte[]) args[1])
    );

    static {
        PARSER.declareString(constructorArg(), KEY_ID_FIELD);
        PARSER.declareField(constructorArg(), (p, c) -> p.binaryValue(), DATA_FIELD, ObjectParser.ValueType.VALUE);
    }

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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(KEY_ID_FIELD.getPreferredName(), keyId);
        builder.field(DATA_FIELD.getPreferredName(), payload);
        builder.endObject();
        return builder;
    }

    public static EncryptedData fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
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
    public String toString() {
        return "EncryptedData{keyId=" + keyId + ", payload=::es_redacted::}";
    }
}
