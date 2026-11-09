/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Serves the project encryption key (PEK) material to {@link AesGcmEncryptionService}.
 *
 * <p>Keys are read on demand from {@link ProjectEncryptionKeyMetadata} in the cluster state visible via {@link ClusterService#state()}.
 * {@link ProjectEncryptionKeyMetadata.KeyEntry#bytes()} holds plaintext AES bytes (the password wrap only applies to the on-disk form).
 * Because {@link ClusterService#state()} may not be read while a cluster state is being applied, encryption and decryption must not be
 * performed from a {@link ClusterStateApplier}.
 *
 * <p>If the elected master published the PEK without key material, this node serves its own {@link ProjectEncryptionKeyMetadata.LocalKeys}
 * copy of the same active key instead. An applier keeps that copy up to date, so it is current before a new state becomes visible.
 *
 * <p>Installation, rotation, and retirement of PEK material are owned by {@link KeyRotationCoordinator}; this class is read-only with
 * respect to cluster state. Password material is supplied via a {@link Supplier} owned by {@link EncryptionPlugin} and updated atomically
 * on secure-settings reload.
 */
class ProjectEncryptionKeyService implements AesGcmEncryptionService.KeyProvider, Closeable {

    private static final Logger logger = LogManager.getLogger(ProjectEncryptionKeyService.class);

    // Prevents key generation in a mixed-version cluster. Without this, TransportVersion filtering
    // would omit the PEK custom from cluster state sent to old nodes (they lack the transport version
    // for it). If an old node becomes master, it sees no existing key and generates a new one,
    // orphaning any data encrypted with the original key.
    static final NodeFeature PROJECT_ENCRYPTION_KEY_FEATURE = new NodeFeature("security.primary_encryption_key");

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final Supplier<Settings> settingsSupplier;
    private final ProjectEncryptionKeyMetadata.LocalKeys localKeys;
    private final ClusterStateApplier localKeysApplier = this::trackLocalKeys;
    private volatile boolean encryptionRequired = true;
    private final Set<String> missingKeyIdWarnings = ConcurrentHashMap.newKeySet();
    // only accessed on the applier thread
    private boolean usingLocalKeysWarned;

    private ProjectEncryptionKeyService(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        Supplier<Settings> settingsSupplier,
        ProjectEncryptionKeyMetadata.LocalKeys localKeys
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.settingsSupplier = settingsSupplier;
        this.localKeys = localKeys;
    }

    static ProjectEncryptionKeyService create(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        Supplier<Settings> settingsSupplier,
        ProjectEncryptionKeyMetadata.LocalKeys localKeys
    ) {
        ProjectEncryptionKeyService service = new ProjectEncryptionKeyService(clusterService, projectResolver, settingsSupplier, localKeys);
        clusterService.addStateApplier(service.localKeysApplier);
        clusterService.getClusterSettings()
            .initializeAndWatch(ProjectEncryptionKeyPasswordSettings.ENCRYPTION_REQUIRED, v -> service.encryptionRequired = v);
        return service;
    }

    @Override
    public void close() {
        clusterService.removeApplier(localKeysApplier);
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
        return ProjectEncryptionKeyPasswordSettings.canWrapForDisk(settingsSupplier.get());
    }

    /** Returns whether this node has a usable project encryption key, from cluster state or its own copy. */
    boolean hasInstalledKey() {
        ProjectEncryptionKeyMetadata metadata = usableMetadata();
        return metadata != null && metadata.isUnwrapFailed() == false;
    }

    /** Returns whether cluster state has no key material but this node still encrypts with its own copy of the keys. */
    boolean isUsingLocalKeys() {
        ProjectEncryptionKeyMetadata metadata = currentMetadata();
        return metadata != null && ownCopyOf(metadata) != null;
    }

    /**
     * Returns the current {@link EncryptionServiceState} of this node based on the visible cluster-state metadata and secure settings.
     */
    public EncryptionServiceState state() {
        ProjectEncryptionKeyMetadata metadata = usableMetadata();
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
        ProjectEncryptionKeyMetadata metadata = usableMetadata();
        if (metadata == null || metadata.isUnwrapFailed()) {
            return null;
        }
        String activeKeyId = metadata.getActiveKeyId();
        return new AesGcmEncryptionService.ActiveKey(activeKeyId, toSecretKey(metadata.getKeys().get(activeKeyId)));
    }

    @Override
    @Nullable
    public SecretKey getKey(String keyId) {
        ProjectEncryptionKeyMetadata metadata = usableMetadata();
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

    private void trackLocalKeys(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
            || event.changedCustomProjectMetadataSet().contains(ProjectEncryptionKeyMetadata.TYPE) == false) {
            return;
        }
        ProjectEncryptionKeyMetadata metadata = projectResolver.getProjectState(state).metadata().custom(ProjectEncryptionKeyMetadata.TYPE);
        localKeys.onApplied(metadata);
        boolean usingLocalKeys = metadata != null && ownCopyOf(metadata) != null;
        if (usingLocalKeys && usingLocalKeysWarned == false) {
            logger.warn(
                "the elected master holds no project encryption keys; this node keeps using its own copy of active key [{}]."
                    + " Configure the encryption password on the elected master and restart it, so that a master-eligible node"
                    + " that holds the keys is elected and restores them. Repeat this if the next elected master holds no keys"
                    + " either",
                metadata.getActiveKeyId()
            );
        }
        usingLocalKeysWarned = usingLocalKeys;
    }

    /** Returns the visible metadata, or this node's own copy of the same active key if the visible metadata has no key material. */
    @Nullable
    private ProjectEncryptionKeyMetadata usableMetadata() {
        ProjectEncryptionKeyMetadata metadata = currentMetadata();
        if (metadata == null) {
            return null;
        }
        ProjectEncryptionKeyMetadata ownCopy = ownCopyOf(metadata);
        return ownCopy != null ? ownCopy : metadata;
    }

    @Nullable
    private ProjectEncryptionKeyMetadata ownCopyOf(ProjectEncryptionKeyMetadata metadata) {
        return metadata.isUnwrapFailed() ? localKeys.lastHealthyWithActiveKey(metadata.getActiveKeyId()) : null;
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
