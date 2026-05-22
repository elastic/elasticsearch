/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.encryption.spi.test;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.security.spi.encryption.EncryptedData;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

/**
 * Minimal {@link Metadata.ProjectCustom} carrying a single {@link EncryptedData} blob. Exists so the integration test can verify the
 * coordinator re-encrypts handler-owned data and updates {@code handlerKeyIds} when the active key rotates.
 */
public final class TestEncryptedBlob extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    public static final String TYPE = "test_encrypted_blob";

    private final EncryptedData blob;

    public TestEncryptedBlob(EncryptedData blob) {
        this.blob = Objects.requireNonNull(blob);
    }

    public TestEncryptedBlob(StreamInput in) throws IOException {
        this.blob = new EncryptedData(in);
    }

    public EncryptedData blob() {
        return blob;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // Test-only; this plugin is loaded only in tests, where the cluster runs the current transport version.
        return TransportVersion.current();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        blob.writeTo(out);
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContentHelper.chunk((builder, ignored) -> builder.field("blob", blob));
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TestEncryptedBlob other && blob.equals(other.blob);
    }

    @Override
    public int hashCode() {
        return blob.hashCode();
    }
}
