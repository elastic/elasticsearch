/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotEncryptedData;
import org.elasticsearch.snapshots.SnapshotGlobalStateTransformer;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;

import java.util.Arrays;
import java.util.function.UnaryOperator;

import javax.crypto.SecretKey;

/**
 * Transforms snapshot global state by re-wrapping PEK-encrypted project customs under a password-derived key. Registered as a
 * {@link SnapshotGlobalStateTransformer} via {@code META-INF/services}.
 *
 * <p>At finalization, {@link #transformForSnapshot} derives a key-encryption-key (KEK) from the snapshot password via PBKDF2 and,
 * for each handler whose custom context includes {@link Metadata.XContentContext#SNAPSHOT}, decrypts each value with the PEK and
 * re-wraps it under the KEK.
 *
 * <p>Without a password every such value is dropped so the snapshot never contains PEK-wrapped data.
 *
 * <p>Customs whose context does not include {@link Metadata.XContentContext#SNAPSHOT}, or whose handler does not write its encrypted
 * values into snapshots, are left untouched.
 */
public final class EncryptingSnapshotGlobalStateTransformer implements SnapshotGlobalStateTransformer {

    private static final Logger logger = LogManager.getLogger(EncryptingSnapshotGlobalStateTransformer.class);

    /** Key ID written into every {@link EncryptedData} value that is re-wrapped under a snapshot password. */
    static final String SNAPSHOT_PASSWORD_KEY_ID = "snapshot_password";

    @Override
    public Metadata transformForSnapshot(ProjectId projectId, Metadata metadata, @Nullable SnapshotEncryptedData encryptedData) {
        if (containsEncryptedData(projectId, metadata) == false) {
            return metadata;
        }
        final EncryptedDataHandlerRegistry registry = EncryptedDataHandlerRegistry.getInstance();
        final ProjectMetadata project = metadata.getProject(projectId);

        if (encryptedData == null) {
            logger.warn(
                "Snapshot will not include encrypted project data for project [{}]: "
                    + "no snapshot password was supplied. Re-configure the data manually after restore.",
                projectId.id()
            );
            return dropEncryptedData(metadata, project, registry);
        }

        if (SnapshotEncryptedData.TYPE_PASSWORD.equals(encryptedData.type()) == false || encryptedData.password() == null) {
            throw new IllegalStateException("unsupported encrypted_data type [" + encryptedData.type() + "]");
        }
        return rewrapEncryptedData(metadata, project, registry, encryptedData.password());
    }

    @Override
    public boolean containsEncryptedData(ProjectId projectId, Metadata metadata) {
        EncryptedDataHandlerRegistry registry = EncryptedDataHandlerRegistry.getInstance();
        if (registry == null) {
            return false;
        }
        ProjectMetadata project = metadata.getProject(projectId);
        if (project == null) {
            return false;
        }
        return registry.handlers()
            .stream()
            .anyMatch(
                handler -> EncryptedDataHandlerRegistry.writtenToSnapshot(handler, project)
                    && EncryptedDataHandlerRegistry.hasData(handler, project)
            );
    }

    private Metadata dropEncryptedData(Metadata metadata, ProjectMetadata project, EncryptedDataHandlerRegistry registry) {
        return applyToSnapshotCustoms(metadata, project, registry, v -> null);
    }

    private Metadata rewrapEncryptedData(
        Metadata metadata,
        ProjectMetadata project,
        EncryptedDataHandlerRegistry registry,
        SecureString password
    ) {
        final EncryptionService encryptionService = EncryptionServiceRegistry.getEncryptionService();
        final byte[] salt = PasswordBasedEncryption.generateSalt();
        final char[] chars = password.getChars().clone();
        final SecretKey kek;
        try {
            kek = PasswordBasedEncryption.deriveKek(chars, salt);
        } finally {
            Arrays.fill(chars, '\0');
        }
        final ProjectEncryptionKeyMetadata capturedKeys = project.custom(ProjectEncryptionKeyMetadata.TYPE);
        return applyToSnapshotCustoms(metadata, project, registry, existing -> {
            final byte[] plaintext;
            try {
                plaintext = encryptionService.decrypt(existing);
            } catch (EncryptionKeyNotYetAvailableException e) {
                throw keyUnavailableAtFinalization(existing.keyId(), capturedKeys, e);
            }
            try {
                return PasswordBasedEncryption.wrapWithKek(plaintext, SNAPSHOT_PASSWORD_KEY_ID, kek, salt);
            } finally {
                Arrays.fill(plaintext, (byte) 0);
            }
        });
    }

    /**
     * A key present in the captured metadata's key set but unknown to the live service was retired while the finalization was queued.
     */
    private static ElasticsearchStatusException keyUnavailableAtFinalization(
        String keyId,
        @Nullable ProjectEncryptionKeyMetadata capturedKeys,
        Exception cause
    ) {
        final String reason = capturedKeys != null && capturedKeys.getKeys().containsKey(keyId)
            ? "project encryption key [{}] was rotated and retired while the snapshot was waiting to finalize"
            : "cannot re-encrypt data under project encryption key [{}] with the snapshot password";
        return new ElasticsearchStatusException(
            reason + ", so the snapshot was not written to the repository and can be retried",
            RestStatus.SERVICE_UNAVAILABLE,
            cause,
            keyId
        );
    }

    private static Metadata applyToSnapshotCustoms(
        Metadata metadata,
        ProjectMetadata project,
        EncryptedDataHandlerRegistry registry,
        UnaryOperator<EncryptedData> rewrapper
    ) {
        ProjectMetadata.Builder projBuilder = null;
        for (EncryptedDataHandler<?> handler : registry.handlers()) {
            projBuilder = applyToSnapshotCustom(projBuilder, project, handler, rewrapper);
        }
        if (projBuilder == null) {
            return metadata;
        }
        return Metadata.builder(metadata).put(projBuilder).build();
    }

    @Nullable
    private static <T extends Metadata.ProjectCustom> ProjectMetadata.Builder applyToSnapshotCustom(
        @Nullable ProjectMetadata.Builder projBuilder,
        ProjectMetadata project,
        EncryptedDataHandler<T> handler,
        UnaryOperator<EncryptedData> rewrapper
    ) {
        if (EncryptedDataHandlerRegistry.writtenToSnapshot(handler, project) == false) {
            return projBuilder;
        }
        T custom = project.custom(handler.customName());
        T result = handler.reEncrypt(custom, rewrapper);
        if (result == custom) {
            return projBuilder;
        }
        if (projBuilder == null) {
            projBuilder = ProjectMetadata.builder(project);
        }
        if (result == null) {
            projBuilder.removeCustom(handler.customName());
        } else {
            projBuilder.putCustom(handler.customName(), result);
        }
        return projBuilder;
    }
}
