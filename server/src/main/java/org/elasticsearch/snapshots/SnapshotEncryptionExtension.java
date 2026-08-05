/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Nullable;

import java.util.Map;

/**
 * SPI for providing snapshot encryption support. Implementations live in the encryption plugin;
 * the default no-op is used when the plugin is absent.
 */
public interface SnapshotEncryptionExtension {

    SnapshotEncryptionExtension NO_OP = new SnapshotEncryptionExtension() {};

    /**
     * Serializes and encrypts any project customs in {@code clusterMetadata} that contain sensitive data,
     * using a key derived from {@code password}. Returns {@code null} if there is nothing to encrypt.
     */
    @Nullable
    default byte[] encryptForSnapshot(ProjectId projectId, Metadata clusterMetadata, char[] password) {
        return null;
    }

    /**
     * Returns {@code true} if {@code clusterMetadata} contains any project custom that holds encrypted data
     * requiring a password to include in a snapshot.
     */
    default boolean hasEncryptedCustoms(ProjectId projectId, Metadata clusterMetadata) {
        return false;
    }

    /**
     * Decrypts {@code encryptedData} using a key derived from {@code password} and returns a map of
     * project-custom type name → restored custom. Returns an empty map if decryption yields nothing.
     */
    default Map<String, Metadata.ProjectCustom> decryptFromSnapshot(ProjectId projectId, byte[] encryptedData, char[] password) {
        return Map.of();
    }
}
