/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.encryption;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

/**
 * Usage of the project encryption key (PEK) feature. {@code has_encrypted_data} reports whether the cluster state
 * contains any PEK-encrypted values; clients (e.g. the Kibana snapshot forms) use it to warn that snapshots taken
 * without {@code encrypted_data} will exclude that data.
 */
public final class EncryptionFeatureSetUsage extends XPackFeatureUsage {

    private static final TransportVersion SNAPSHOT_ENCRYPTED_DATA = TransportVersion.fromName("snapshot_encrypted_data");

    private final boolean hasEncryptedData;

    public EncryptionFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        hasEncryptedData = input.readBoolean();
    }

    public EncryptionFeatureSetUsage(boolean enabled, boolean hasEncryptedData) {
        super(XPackField.ENCRYPTION, true, enabled);
        this.hasEncryptedData = hasEncryptedData;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(hasEncryptedData);
    }

    public boolean hasEncryptedData() {
        return hasEncryptedData;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return SNAPSHOT_ENCRYPTED_DATA;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("has_encrypted_data", hasEncryptedData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, hasEncryptedData);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EncryptionFeatureSetUsage other = (EncryptionFeatureSetUsage) obj;
        return available == other.available && enabled == other.enabled && hasEncryptedData == other.hasEncryptedData;
    }
}
