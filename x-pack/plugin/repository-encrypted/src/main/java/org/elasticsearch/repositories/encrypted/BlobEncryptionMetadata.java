package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class BlobEncryptionMetadata {

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

    public void write(OutputStream outputStream) throws IOException {
        try (StreamOutput out = new OutputStreamStreamOutput(outputStream)) {
            out.setVersion(Version.CURRENT);
            out.writeByteArray(this.dataEncryptionKeyMaterial);
            out.writeInt(this.nonce);
            out.writeInt(this.packetLengthInBytes);
        }
    }

}
