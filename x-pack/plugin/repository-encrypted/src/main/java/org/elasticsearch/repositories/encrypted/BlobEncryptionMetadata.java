/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class BlobEncryptionMetadata implements Writeable {

    private final byte[] dataEncryptionKeyMaterial;
    private final int nonce;
    private final int packetLengthInBytes;

    public BlobEncryptionMetadata(byte[] dataEncryptionKeyMaterial, int nonce, int packetLengthInBytes) {
        this.dataEncryptionKeyMaterial = dataEncryptionKeyMaterial;
        this.nonce = nonce;
        this.packetLengthInBytes = packetLengthInBytes;
    }

    public byte[] getDataEncryptionKeyMaterial() {
        return dataEncryptionKeyMaterial;
    }

    public int getPacketLengthInBytes() {
        return packetLengthInBytes;
    }

    public int getNonce() {
        return nonce;
    }

    public BlobEncryptionMetadata(InputStream inputStream) throws IOException {
        try (StreamInput in = new InputStreamStreamInput(inputStream)) {
            final Version version = Version.readVersion(in);
            in.setVersion(version);
            this.dataEncryptionKeyMaterial = in.readByteArray();
            this.nonce = in.readInt();
            this.packetLengthInBytes = in.readInt();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.setVersion(Version.CURRENT);
        out.writeByteArray(this.dataEncryptionKeyMaterial);
        out.writeInt(this.nonce);
        out.writeInt(this.packetLengthInBytes);
    }
}
