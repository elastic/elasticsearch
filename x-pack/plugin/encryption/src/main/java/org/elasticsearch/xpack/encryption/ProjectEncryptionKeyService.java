/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Serves the project encryption key (PEK) material to {@link AesGcmEncryptionService}.
 *
 * <p>Keys are read on demand from {@link ProjectEncryptionKeyMetadata} in the cluster state visible via {@link ClusterService#state()}.
 * {@link ProjectEncryptionKeyMetadata.KeyEntry#bytes()} holds plaintext AES bytes (the password wrap only applies to the on-disk form), so
 * no local copy of the keys is kept. Because {@link ClusterService#state()} may not be read while a cluster state is being applied,
 * encryption and decryption must not be performed from a {@link org.elasticsearch.cluster.ClusterStateApplier}.
 *
 * <p>Installation, rotation, and retirement of PEK material are owned by {@link KeyRotationCoordinator}; this class is read-only with
 * respect to cluster state. Password material is supplied via a {@link Supplier} owned by {@link EncryptionPlugin} and updated atomically
 * on secure-settings reload.
 */
class ProjectEncryptionKeyService implements AesGcmEncryptionService.KeyProvider {

    private static final Logger logger = LogManager.getLogger(ProjectEncryptionKeyService.class);

    // Prevents key generation in a mixed-version cluster. Without this, TransportVersion filtering
    // would omit the PEK custom from cluster state sent to old nodes (they lack the transport version
    // for it). If an old node becomes master, it sees no existing key and generates a new one,
    // orphaning any data encrypted with the original key.
    static final NodeFeature PROJECT_ENCRYPTION_KEY_FEATURE = new NodeFeature("security.primary_encryption_key");

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final Supplier<Settings> settingsSupplier;
    private volatile boolean encryptionRequired = true;
    private final Set<String> missingKeyIdWarnings = ConcurrentHashMap.newKeySet();

    private ProjectEncryptionKeyService(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        Supplier<Settings> settingsSupplier
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.settingsSupplier = settingsSupplier;
    }

    static ProjectEncryptionKeyService create(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        Supplier<Settings> settingsSupplier
    ) {
        ProjectEncryptionKeyService service = new ProjectEncryptionKeyService(clusterService, projectResolver, settingsSupplier);
        clusterService.getClusterSettings()
            .initializeAndWatch(ProjectEncryptionKeyPasswordSettings.ENCRYPTION_REQUIRED, v -> service.encryptionRequired = v);
        return service;
    }

    /**
     * Returns the active password id from the current password settings snapshot. May be {@code null} if no active password is configured.
     */
    @Nullable
    String getActivePasswordId() {
        return ProjectEncryptionKeyPasswordSettings.getActivePasswordId(settingsSupplier.get());
    }

    /**
     * Returns whether this node can wrap the PEK for its next on-disk cluster-state persist, i.e., an active password id is configured
     * and the matching password material is present in the current settings snapshot.
     */
    boolean canWrapForDisk() {
        Settings s = settingsSupplier.get();
        String id = ProjectEncryptionKeyPasswordSettings.getActivePasswordId(s);
        return ProjectEncryptionKeyPasswordSettings.hasPassword(s, id);
    }

    /**
     * Returns the current {@link EncryptionServiceState} of this node based on the visible cluster-state metadata and secure settings.
     */
    public EncryptionServiceState state() {
        ProjectEncryptionKeyMetadata metadata = currentMetadata();
        if (metadata == null) {
            // Awaiting first key install. DISABLED only when no password is configured at all; otherwise READY
            // (the coordinator will install the first key shortly after the password appears in settings).
            return getActivePasswordId() == null ? EncryptionServiceState.DISABLED : EncryptionServiceState.READY;
        }
        if (metadata.isUnwrapFailed()) {
            return EncryptionServiceState.UNAVAILABLE_DECRYPTION_FAILED;
        }
        // Keys are carried in plaintext over the wire (since PRIMARY_ENCRYPTION_KEY_CLEARTEXT_TRANSPORT), so
        // once a key is installed the node can always encrypt/decrypt without a password.
        return EncryptionServiceState.READY;
    }

    /** Returns whether callers must refuse to store secrets when the service is not {@link EncryptionServiceState#READY}. */
    public boolean isEncryptionRequired() {
        return encryptionRequired;
    }

    @Override
    @Nullable
    public AesGcmEncryptionService.ActiveKey getActiveKey() {
        ProjectEncryptionKeyMetadata metadata = currentMetadata();
        if (metadata == null || metadata.isUnwrapFailed()) {
            return null;
        }
        String activeKeyId = metadata.getActiveKeyId();
        return new AesGcmEncryptionService.ActiveKey(activeKeyId, toSecretKey(metadata.getKeys().get(activeKeyId)));
    }

    @Override
    @Nullable
    public SecretKey getKey(String keyId) {
        ProjectEncryptionKeyMetadata metadata = currentMetadata();
        if (metadata == null || metadata.isUnwrapFailed()) {
            return null;
        }
        ProjectEncryptionKeyMetadata.KeyEntry entry = metadata.getKeys().get(keyId);
        if (entry == null) {
            if (missingKeyIdWarnings.add(keyId)) {
                logger.warn("project encryption key [{}] is not in cluster state (active key is [{}])", keyId, metadata.getActiveKeyId());
            }
            return null;
        }
        return toSecretKey(entry);
    }

    @Nullable
    private ProjectEncryptionKeyMetadata currentMetadata() {
        ClusterState state = clusterService.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return null;
        }
        ProjectMetadata project = state.metadata().projects().get(projectResolver.getProjectId());
        return project == null ? null : project.custom(ProjectEncryptionKeyMetadata.TYPE);
    }

    private static SecretKey toSecretKey(ProjectEncryptionKeyMetadata.KeyEntry entry) {
        return new SecretKeySpec(entry.bytes(), "AES");
    }
}
