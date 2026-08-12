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
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.SnapshotGlobalStateTransformer;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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

    /**
     * When {@code true}, creating a snapshot whose global state contains encrypted data fails unless the request
     * carries an {@code encrypted_data} object. When {@code false} (the default) such data is silently excluded
     * from the snapshot, with a log and deprecation warning. Inert for clusters without encrypted data.
     */
    public static final Setting<Boolean> ENCRYPTED_DATA_REQUIRED_SETTING = Setting.boolSetting(
        "snapshot.encrypted_data.required",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(EncryptingSnapshotGlobalStateTransformer.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(EncryptingSnapshotGlobalStateTransformer.class);

    private static final String NO_PASSWORD_WARNING = "Encrypted data exists but no password was provided; "
        + "it was excluded from the snapshot. Provide encrypted_data to include it.";

    // instantiated via SPI without constructor dependencies, so the dynamic setting is published into a static slot
    private static volatile boolean encryptedDataRequired = ENCRYPTED_DATA_REQUIRED_SETTING.getDefault(Settings.EMPTY);

    static void setEncryptedDataRequired(boolean value) {
        encryptedDataRequired = value;
    }

    public EncryptingSnapshotGlobalStateTransformer() {}

    @Override
    @SuppressWarnings("unchecked")
    public Metadata transformForSnapshot(ProjectId projectId, Metadata metadata, @Nullable CreateSnapshotRequest request) {
        final ProjectMetadata project = metadata.getProject(projectId);
        final var encryptionService = EncryptionServiceRegistry.getEncryptionService();
        final SecureString secret = request == null || request.encryptedData() == null ? null : request.encryptedData().password();

        final Map<String, Metadata.ProjectCustom> transformed = new HashMap<>();
        for (EncryptedDataHandler<?> rawHandler : EncryptedDataHandlerRegistry.getInstance().handlers()) {
            final EncryptedDataHandler<Metadata.ProjectCustom> handler = (EncryptedDataHandler<Metadata.ProjectCustom>) rawHandler;
            final Metadata.ProjectCustom custom = project.custom(handler.customName());
            // skip customs that will not be serialized into the snapshot's global state (context filtering
            // happens at write time, so the metadata passed here may still contain e.g. gateway-only customs)
            if (custom == null || custom.context().contains(Metadata.XContentContext.SNAPSHOT) == false) {
                continue;
            }
            // with a password, re-wrap each value under it; without one, exclude the values so the snapshot
            // never contains data wrapped under this cluster's project encryption key
            final Metadata.ProjectCustom result = handler.reEncrypt(custom, secret == null ? existing -> null : existing -> {
                byte[] plaintext = encryptionService.decrypt(existing);
                try {
                    return PasswordBasedEncryption.wrap(plaintext, SNAPSHOT_PASSWORD_KEY_ID, secret.getChars());
                } finally {
                    Arrays.fill(plaintext, (byte) 0);
                }
            });
            if (result != custom) {
                transformed.put(handler.customName(), result);
            }
        }

        if (secret == null && transformed.isEmpty() == false) {
            if (encryptedDataRequired) {
                throw new IllegalArgumentException(
                    "cannot create snapshot: the cluster contains encrypted data, ["
                        + ENCRYPTED_DATA_REQUIRED_SETTING.getKey()
                        + "] is set to [true], and no encrypted_data was provided"
                );
            }
            logger.warn(NO_PASSWORD_WARNING);
            deprecationLogger.warn(DeprecationCategory.OTHER, "snapshot_encrypted_data_missing", NO_PASSWORD_WARNING);
        }
        if (secret != null && transformed.isEmpty()) {
            logger.warn("encrypted_data was provided but the snapshot global state contains no encrypted data");
        }
        return transformed.isEmpty() ? metadata : withReplacedCustoms(metadata, project, transformed);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Metadata transformForRestore(ProjectId projectId, Metadata restored, RestoreSnapshotRequest request) {
        final ProjectMetadata project = restored.getProject(projectId);
        final var encryptionService = EncryptionServiceRegistry.getEncryptionService();
        final SecureString secret = request.encryptedData() == null ? null : request.encryptedData().password();

        final boolean[] excludedForMissingPassword = new boolean[1];
        final Map<String, Metadata.ProjectCustom> rewrapped = new HashMap<>();
        for (EncryptedDataHandler<?> rawHandler : EncryptedDataHandlerRegistry.getInstance().handlers()) {
            final EncryptedDataHandler<Metadata.ProjectCustom> handler = (EncryptedDataHandler<Metadata.ProjectCustom>) rawHandler;
            final Metadata.ProjectCustom custom = project.custom(handler.customName());
            if (custom == null) {
                continue;
            }
            final Metadata.ProjectCustom result = handler.reEncrypt(custom, existing -> {
                if (SNAPSHOT_PASSWORD_KEY_ID.equals(existing.keyId()) == false) {
                    // snapshots only ever contain password-wrapped values; anything else is foreign or corrupt
                    logger.warn(
                        "restored [{}] contains a value encrypted under unknown key [{}]; the value is cleared",
                        handler.customName(),
                        existing.keyId()
                    );
                    return null;
                }
                if (secret == null) {
                    excludedForMissingPassword[0] = true;
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

        if (excludedForMissingPassword[0]) {
            logger.warn(
                "Encrypted data exists but no password was provided; it was excluded when restoring the snapshot. "
                    + "Provide encrypted_data to include it."
            );
        }
        if (secret != null && rewrapped.isEmpty()) {
            logger.warn("encrypted_data was provided but the restored global state contains no encrypted data");
        }
        return rewrapped.isEmpty() ? restored : withReplacedCustoms(restored, project, rewrapped);
    }

    private static Metadata withReplacedCustoms(Metadata metadata, ProjectMetadata project, Map<String, Metadata.ProjectCustom> customs) {
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(project);
        customs.forEach(projectBuilder::putCustom);
        return Metadata.builder(metadata).put(projectBuilder).build();
    }
}
