/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.SnapshotEncryptedData;
import org.elasticsearch.snapshots.SnapshotGlobalStateTransformer;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;

import java.util.Arrays;
import java.util.function.UnaryOperator;

import javax.crypto.SecretKey;

/**
 * Transforms snapshot global state by re-wrapping PEK-encrypted project customs under a
 * password-derived key. Registered as a {@link SnapshotGlobalStateTransformer} via
 * {@code META-INF/services}.
 *
 * <p>When {@code encryptedData} carries a password the transformer:
 * <ol>
 *   <li>Derives a key-encryption-key (KEK) once from the password via PBKDF2.</li>
 *   <li>For each handler whose custom context includes {@link Metadata.XContentContext#SNAPSHOT},
 *       decrypts each value with the PEK and re-wraps it under the KEK.</li>
 * </ol>
 *
 * <p>When {@code encryptedData} is {@code null} (master failover or no request), every value is
 * dropped so the snapshot never contains PEK-wrapped data.
 *
 * <p>Customs whose context does not include {@link Metadata.XContentContext#SNAPSHOT} are left
 * untouched — the repository writer will not serialise them at all.
 */
public final class EncryptingSnapshotGlobalStateTransformer implements SnapshotGlobalStateTransformer {

    private static final Logger logger = LogManager.getLogger(EncryptingSnapshotGlobalStateTransformer.class);

    /** Key ID written into every {@link EncryptedData} value that is re-wrapped under a snapshot password. */
    static final String SNAPSHOT_PASSWORD_KEY_ID = "snapshot_password";

    @Override
    public Metadata transformForSnapshot(ProjectId projectId, Metadata metadata, @Nullable SnapshotEncryptedData encryptedData) {
        EncryptedDataHandlerRegistry registry = EncryptedDataHandlerRegistry.getInstance();
        if (registry.handlers().isEmpty()) {
            return metadata;
        }

        final ProjectMetadata project = metadata.getProject(projectId);
        if (project == null) {
            return metadata;
        }

        // Skip customs that are not in SNAPSHOT context — the repository layer won't write them anyway.
        boolean anySnapshotCustom = false;
        for (EncryptedDataHandler<?> handler : registry.handlers()) {
            Metadata.ProjectCustom custom = project.custom(handler.customName());
            if (custom != null && custom.context().contains(Metadata.XContentContext.SNAPSHOT)) {
                anySnapshotCustom = true;
                break;
            }
        }
        if (anySnapshotCustom == false) {
            return metadata;
        }

        if (encryptedData == null || encryptedData.password() == null) {
            // No password — drop all encrypted project data from the snapshot.
            logger.warn(
                "Snapshot will not include encrypted project data for project [{}]: "
                    + "no snapshot password was supplied. Re-configure the data manually after restore.",
                projectId.id()
            );
            return dropEncryptedData(projectId, metadata, project, registry);
        }

        SnapshotEncryptedData.validatePassword(encryptedData.password());
        return rewrapEncryptedData(projectId, metadata, project, registry, encryptedData);
    }

    @Override
    public boolean containsEncryptedData(ProjectId projectId, Metadata metadata) {
        EncryptedDataHandlerRegistry registry;
        try {
            registry = EncryptedDataHandlerRegistry.getInstance();
        } catch (IllegalStateException e) {
            return false;
        }
        ProjectMetadata project = metadata.getProject(projectId);
        if (project == null) {
            return false;
        }
        for (EncryptedDataHandler<?> handler : registry.handlers()) {
            Metadata.ProjectCustom custom = project.custom(handler.customName());
            if (custom != null && custom.context().contains(Metadata.XContentContext.SNAPSHOT)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Metadata dropEncryptedData(
        ProjectId projectId,
        Metadata metadata,
        ProjectMetadata project,
        EncryptedDataHandlerRegistry registry
    ) {
        ProjectMetadata.Builder projBuilder = null;
        for (EncryptedDataHandler<?> handler : registry.handlers()) {
            Metadata.ProjectCustom custom = project.custom(handler.customName());
            if (custom == null || custom.context().contains(Metadata.XContentContext.SNAPSHOT) == false) {
                continue;
            }
            Metadata.ProjectCustom result = ((EncryptedDataHandler) handler).reEncrypt(custom, (UnaryOperator<EncryptedData>) v -> null);
            if (result != custom) {
                if (projBuilder == null) {
                    projBuilder = ProjectMetadata.builder(project);
                }
                if (result == null) {
                    projBuilder.removeCustom(handler.customName());
                } else {
                    projBuilder.putCustom(handler.customName(), result);
                }
            }
        }
        if (projBuilder == null) {
            return metadata;
        }
        return Metadata.builder(metadata).put(projBuilder).build();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Metadata rewrapEncryptedData(
        ProjectId projectId,
        Metadata metadata,
        ProjectMetadata project,
        EncryptedDataHandlerRegistry registry,
        SnapshotEncryptedData encryptedData
    ) {
        EncryptionService encryptionService = EncryptionServiceRegistry.getEncryptionService();

        // Derive the KEK once so PBKDF2 is paid once per snapshot, not once per encrypted value.
        // All values in this snapshot share the same salt — intentional and safe within a single snapshot.
        final char[] password = encryptedData.password().getChars().clone();
        final byte[] salt = PasswordBasedEncryption.generateSalt();
        try {
            final SecretKey kek = PasswordBasedEncryption.deriveKek(password, salt);
            final UnaryOperator<EncryptedData> rewrapper = existing -> {
                byte[] plaintext = encryptionService.decrypt(existing);
                try {
                    return PasswordBasedEncryption.wrapWithKek(plaintext, SNAPSHOT_PASSWORD_KEY_ID, kek, salt);
                } finally {
                    Arrays.fill(plaintext, (byte) 0);
                }
            };

            ProjectMetadata.Builder projBuilder = null;
            for (EncryptedDataHandler<?> handler : registry.handlers()) {
                Metadata.ProjectCustom custom = project.custom(handler.customName());
                if (custom == null || custom.context().contains(Metadata.XContentContext.SNAPSHOT) == false) {
                    continue;
                }
                Metadata.ProjectCustom result = ((EncryptedDataHandler) handler).reEncrypt(custom, rewrapper);
                if (result != custom) {
                    if (projBuilder == null) {
                        projBuilder = ProjectMetadata.builder(project);
                    }
                    if (result == null) {
                        projBuilder.removeCustom(handler.customName());
                    } else {
                        projBuilder.putCustom(handler.customName(), result);
                    }
                }
            }
            if (projBuilder == null) {
                return metadata;
            }
            return Metadata.builder(metadata).put(projBuilder).build();
        } finally {
            Arrays.fill(password, '\0');
        }
    }
}
