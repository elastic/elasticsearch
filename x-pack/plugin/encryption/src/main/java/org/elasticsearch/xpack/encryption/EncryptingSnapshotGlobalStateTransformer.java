/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.SnapshotGlobalStateTransformer;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements {@link SnapshotGlobalStateTransformer} by re-wrapping {@link org.elasticsearch.xpack.core.encryption.EncryptedData}
 * values in place: at snapshot time each handler re-encrypts its custom's PEK-wrapped values under a key derived from the
 * snapshot password; at restore time password-wrapped values are re-encrypted under the destination cluster's PEK. The
 * customs' types and schemas never change, so the values simply travel inside the snapshot's regular global-state blob.
 *
 * <p>Instantiated via the service-provider mechanism (see {@code META-INF/services}), so it has no constructor dependencies;
 * the handler registry and encryption service are resolved from their static slots at transform time, long after node startup.
 */
public final class EncryptingSnapshotGlobalStateTransformer implements SnapshotGlobalStateTransformer {

    /**
     * The {@code EncryptedData.keyId()} marking a value wrapped under a snapshot-password-derived key rather than under
     * a project encryption key. Such values can only be recovered by a restore supplying the same password.
     */
    public static final String SNAPSHOT_PASSWORD_KEY_ID = "snapshot_password";

    private static final Logger logger = LogManager.getLogger(EncryptingSnapshotGlobalStateTransformer.class);

    public EncryptingSnapshotGlobalStateTransformer() {}

    @Override
    @SuppressWarnings("unchecked")
    public Metadata transformForSnapshot(ProjectId projectId, Metadata metadata, @Nullable CreateSnapshotRequest request) {
        final ProjectMetadata project = metadata.getProject(projectId);
        final var encryptionService = EncryptionServiceRegistry.getEncryptionService();
        final SecureString secret = request == null ? null : request.encryptionPassword();

        boolean hasEncryptedData = false;
        final Map<String, Metadata.ProjectCustom> rewrapped = new HashMap<>();
        for (EncryptedDataHandler<?> rawHandler : EncryptedDataHandlerRegistry.getInstance().handlers()) {
            final EncryptedDataHandler<Metadata.ProjectCustom> handler = (EncryptedDataHandler<Metadata.ProjectCustom>) rawHandler;
            final Metadata.ProjectCustom custom = project.custom(handler.customName());
            // skip customs that will not be serialized into the snapshot's global state (context filtering
            // happens at write time, so the metadata passed here may still contain e.g. gateway-only customs)
            if (custom == null || custom.context().contains(Metadata.XContentContext.SNAPSHOT) == false) {
                continue;
            }
            if (secret == null) {
                hasEncryptedData |= containsEncryptedData(handler, custom);
            } else {
                final Metadata.ProjectCustom result = handler.reEncrypt(custom, existing -> {
                    byte[] plaintext = encryptionService.decrypt(existing);
                    try {
                        return PasswordBasedEncryption.wrap(plaintext, SNAPSHOT_PASSWORD_KEY_ID, secret.getChars());
                    } finally {
                        Arrays.fill(plaintext, (byte) 0);
                    }
                });
                if (result != custom) {
                    rewrapped.put(handler.customName(), result);
                }
            }
        }

        if (hasEncryptedData && secret == null) {
            logger.warn(
                "snapshot global state contains encrypted data but no encryption_password was provided; "
                    + "the encrypted data will only be restorable on a cluster holding the same project encryption key"
            );
        }
        if (secret != null && rewrapped.isEmpty()) {
            logger.warn("an encryption_password was provided but the snapshot global state contains no encrypted data");
        }
        return rewrapped.isEmpty() ? metadata : withReplacedCustoms(metadata, project, rewrapped);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Metadata transformForRestore(ProjectId projectId, Metadata restored, RestoreSnapshotRequest request) {
        final ProjectMetadata project = restored.getProject(projectId);
        final var encryptionService = EncryptionServiceRegistry.getEncryptionService();
        final SecureString secret = request.encryptionPassword();

        final Map<String, Metadata.ProjectCustom> rewrapped = new HashMap<>();
        for (EncryptedDataHandler<?> rawHandler : EncryptedDataHandlerRegistry.getInstance().handlers()) {
            final EncryptedDataHandler<Metadata.ProjectCustom> handler = (EncryptedDataHandler<Metadata.ProjectCustom>) rawHandler;
            final Metadata.ProjectCustom custom = project.custom(handler.customName());
            if (custom == null) {
                continue;
            }
            final Metadata.ProjectCustom result = handler.reEncrypt(custom, existing -> {
                if (SNAPSHOT_PASSWORD_KEY_ID.equals(existing.keyId()) == false) {
                    // PEK-wrapped: a same-cluster restore needs no password
                    return existing;
                }
                if (secret == null) {
                    logger.warn(
                        "restored [{}] contains a value encrypted with a snapshot encryption_password but none was provided; "
                            + "the value is cleared",
                        handler.customName()
                    );
                    return null;
                }
                byte[] plaintext = PasswordBasedEncryption.unwrap(existing, secret.getChars());
                try {
                    return encryptionService.encrypt(plaintext);
                } finally {
                    Arrays.fill(plaintext, (byte) 0);
                }
            });
            if (result != custom) {
                rewrapped.put(handler.customName(), result);
            }
        }

        return rewrapped.isEmpty() ? restored : withReplacedCustoms(restored, project, rewrapped);
    }

    private static boolean containsEncryptedData(EncryptedDataHandler<Metadata.ProjectCustom> handler, Metadata.ProjectCustom custom) {
        AtomicBoolean found = new AtomicBoolean();
        handler.reEncrypt(custom, existing -> {
            found.set(true);
            return existing;
        });
        return found.get();
    }

    private static Metadata withReplacedCustoms(Metadata metadata, ProjectMetadata project, Map<String, Metadata.ProjectCustom> customs) {
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(project);
        customs.forEach(projectBuilder::putCustom);
        return Metadata.builder(metadata).put(projectBuilder).build();
    }
}
