/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.SnapshotEncryptionExtension;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements {@link SnapshotEncryptionExtension} using the encryption plugin's handler registry and
 * password-based encryption. Each handler that returns non-null bytes from {@link EncryptedDataHandler#toSnapshotBytes}
 * contributes a name+bytes pair; the concatenated pairs are then wrapped with {@link PasswordBasedEncryption}.
 */
public final class SnapshotEncryptionExtensionImpl implements SnapshotEncryptionExtension {

    private final EncryptedDataHandlerRegistry handlerRegistry;

    public SnapshotEncryptionExtensionImpl(EncryptedDataHandlerRegistry handlerRegistry) {
        this.handlerRegistry = handlerRegistry;
    }

    @Override
    public boolean hasEncryptedCustoms(ProjectId projectId, Metadata clusterMetadata) {
        final ProjectMetadata project = clusterMetadata.getProject(projectId);
        for (EncryptedDataHandler<?> handler : handlerRegistry.handlers()) {
            if (project.custom(handler.customName()) != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    @Nullable
    @SuppressWarnings("unchecked")
    public byte[] encryptForSnapshot(ProjectId projectId, Metadata clusterMetadata, char[] password) {
        final ProjectMetadata project = clusterMetadata.getProject(projectId);
        final var encryptionService = EncryptionServiceRegistry.getEncryptionService();

        final List<Map.Entry<String, byte[]>> entries = new ArrayList<>();
        for (EncryptedDataHandler<?> rawHandler : handlerRegistry.handlers()) {
            final EncryptedDataHandler<Metadata.ProjectCustom> handler = (EncryptedDataHandler<Metadata.ProjectCustom>) rawHandler;
            final Metadata.ProjectCustom custom = project.custom(handler.customName());
            if (custom == null) {
                continue;
            }
            final byte[] handlerBytes = handler.toSnapshotBytes(custom, encryptionService);
            if (handlerBytes != null) {
                entries.add(Map.entry(handler.customName(), handlerBytes));
            }
        }
        if (entries.isEmpty()) {
            return null;
        }

        final byte[] plaintext;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(entries.size());
            for (Map.Entry<String, byte[]> entry : entries) {
                out.writeString(entry.getKey());
                out.writeByteArray(entry.getValue());
            }
            plaintext = out.bytes().array();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to serialize encrypted snapshot customs", e);
        }

        final EncryptedData wrapped = PasswordBasedEncryption.wrap(plaintext, "snapshot", password);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wrapped.writeTo(out);
            return out.bytes().array();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to serialize EncryptedData for snapshot", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Metadata.ProjectCustom> decryptFromSnapshot(ProjectId projectId, byte[] encryptedData, char[] password) {
        final EncryptedData wrapped;
        try (StreamInput in = StreamInput.wrap(encryptedData)) {
            wrapped = new EncryptedData(in);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to deserialize EncryptedData from snapshot blob", e);
        }

        final byte[] plaintext = PasswordBasedEncryption.unwrap(wrapped, password);

        final var encryptionService = EncryptionServiceRegistry.getEncryptionService();
        final Map<String, EncryptedDataHandler<Metadata.ProjectCustom>> handlerByName = new HashMap<>();
        for (EncryptedDataHandler<?> rawHandler : handlerRegistry.handlers()) {
            handlerByName.put(rawHandler.customName(), (EncryptedDataHandler<Metadata.ProjectCustom>) rawHandler);
        }

        final Map<String, Metadata.ProjectCustom> result = new HashMap<>();
        try (StreamInput in = StreamInput.wrap(plaintext)) {
            int count = in.readVInt();
            for (int i = 0; i < count; i++) {
                final String name = in.readString();
                final byte[] bytes = in.readByteArray();
                final EncryptedDataHandler<Metadata.ProjectCustom> handler = handlerByName.get(name);
                if (handler != null) {
                    final Metadata.ProjectCustom custom = handler.fromSnapshotBytes(bytes, encryptionService);
                    if (custom != null) {
                        result.put(name, custom);
                    }
                }
            }
        } catch (IOException e) {
            throw new ElasticsearchException("failed to deserialize snapshot customs from encrypted blob", e);
        }
        return result;
    }
}
