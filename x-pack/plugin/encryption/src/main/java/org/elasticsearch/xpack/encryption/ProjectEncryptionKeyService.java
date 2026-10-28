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
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Owns the in-memory cache of the project encryption key (PEK) material and serves it to {@link AesGcmEncryptionService}.
 *
 * <p>This service rebuilds its local plaintext-key cache whenever {@link ProjectEncryptionKeyMetadata} changes. It is a cluster-state
 * applier so keys are cached before a new state becomes visible via {@link ClusterService#state()}. Installation, rotation, and
 * retirement of PEK material are owned by {@link KeyRotationCoordinator}; this class is read-only with respect to cluster state.
 *
 * <p>Because {@link ProjectEncryptionKeyMetadata.KeyEntry#bytes()} holds plaintext AES bytes, keys are available immediately on each
 * cluster-state update without a separate unwrap step. Password material is supplied via a {@link Supplier} owned by
 * {@link EncryptionPlugin} and updated atomically on secure-settings reload; the in-memory key cache is unaffected by password changes.
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
    private final ClusterStateApplier clusterStateApplier = this::onClusterStateChanged;
    private final Supplier<Settings> settingsSupplier;
    private volatile KeyCache cache = KeyCache.EMPTY;
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
        clusterService.addStateApplier(service.clusterStateApplier);
        clusterService.getClusterSettings()
            .initializeAndWatch(ProjectEncryptionKeyPasswordSettings.ENCRYPTION_REQUIRED, v -> service.encryptionRequired = v);
        return service;
    }

    @Override
    public void close() {
        clusterService.removeApplier(clusterStateApplier);
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
     * Returns the current {@link EncryptionServiceState} of this node based on the cached cluster-state metadata and secure settings.
     * This is a read-only snapshot — it does not trigger key unwrapping.
     */
    public EncryptionServiceState state() {
        KeyCache snapshot = cache;
        if (snapshot.unavailableState() != null) {
            return snapshot.unavailableState();
        }
        if (snapshot.activeKeyId == null) {
            // Awaiting first key install. DISABLED only when no password is configured at all; otherwise READY
            // (the coordinator will install the first key shortly after the password appears in settings).
            return getActivePasswordId() == null ? EncryptionServiceState.DISABLED : EncryptionServiceState.READY;
        }
        // Keys are carried in plaintext over the wire (since PRIMARY_ENCRYPTION_KEY_CLEARTEXT_TRANSPORT), so
        // once a key is installed the node can always encrypt/decrypt without a password.
        return EncryptionServiceState.READY;
    }

    /** Returns whether callers must refuse to store secrets when the service is not {@link EncryptionServiceState#READY}. */
    public boolean isEncryptionRequired() {
        return encryptionRequired;
    }

    private void onClusterStateChanged(ClusterChangedEvent event) {
        try {
            updateCache(event);
        } catch (Exception e) {
            // appliers must not throw, so degrade encryption on this node rather than fail cluster state application
            logger.warn("project encryption key: failed to load key from cluster state, encryption/decryption unavailable", e);
            this.cache = new KeyCache(null, Map.of(), EncryptionServiceState.UNAVAILABLE_KEY_LOAD_FAILED, event.state().version());
        }
    }

    private void updateCache(ClusterChangedEvent event) {
        ClusterState state = event.state();

        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            logger.debug("project encryption key: cluster not recovered yet, skipping");
            return;
        }

        if (event.changedCustomProjectMetadataSet().contains(ProjectEncryptionKeyMetadata.TYPE) == false) {
            return;
        }

        ProjectState projectState = projectResolver.getProjectState(state);
        ProjectEncryptionKeyMetadata metadata = projectState.metadata().custom(ProjectEncryptionKeyMetadata.TYPE);

        long version = state.version();
        if (metadata == null) {
            if (cache.activeKeyId() != null || cache.unavailableState() != null) {
                logger.debug("project encryption key cache cleared after metadata removed");
            }
            this.cache = new KeyCache(null, Map.of(), null, version);
            return;
        }

        if (metadata.isUnwrapFailed()) {
            if (cache.unavailableState() != EncryptionServiceState.UNAVAILABLE_DECRYPTION_FAILED) {
                logger.warn(
                    "project encryption key: disk recovery failed (degraded state), encryption/decryption unavailable."
                        + " To recover: fix the password and restart, or call POST /_encryption/_reset?accept_data_loss=true"
                );
            }
            this.cache = new KeyCache(null, Map.of(), EncryptionServiceState.UNAVAILABLE_DECRYPTION_FAILED, version);
            return;
        }

        Map<String, SecretKey> decryptedKeys = HashMap.newHashMap(metadata.getKeys().size());
        for (Map.Entry<String, ProjectEncryptionKeyMetadata.KeyEntry> entry : metadata.getKeys().entrySet()) {
            decryptedKeys.put(entry.getKey(), new SecretKeySpec(entry.getValue().bytes(), "AES"));
        }
        this.cache = new KeyCache(metadata.getActiveKeyId(), Map.copyOf(decryptedKeys), null, version);
        logger.debug("project encryption key cache updated: activeKeyId={}", metadata.getActiveKeyId());
    }

    @Override
    @Nullable
    public AesGcmEncryptionService.ActiveKey getActiveKey() {
        KeyCache snapshot = cache;
        if (snapshot.activeKeyId == null) {
            checkMissingKey(null);
            return null;
        }
        return new AesGcmEncryptionService.ActiveKey(snapshot.activeKeyId, snapshot.decryptedKeys.get(snapshot.activeKeyId));
    }

    @Override
    @Nullable
    public SecretKey getKey(String keyId) {
        KeyCache snapshot = cache;
        SecretKey key = snapshot.decryptedKeys.get(keyId);
        if (key == null) {
            checkMissingKey(keyId);
        }
        return key;
    }

    /** Asserts the cache doesn't lag the visible cluster state and warns once per key that is missing from it. */
    private void checkMissingKey(@Nullable String keyId) {
        ClusterState visible = clusterService.state();
        // read after the visible state: the applier updates the cache before publishing, so this is at least as new
        KeyCache current = cache;
        ProjectMetadata project = visible.metadata().projects().get(projectResolver.getProjectId());
        ProjectEncryptionKeyMetadata metadata = project == null ? null : project.custom(ProjectEncryptionKeyMetadata.TYPE);
        if (metadata == null || metadata.isUnwrapFailed() || current.unavailableState() != null) {
            return;
        }
        String id = keyId != null ? keyId : metadata.getActiveKeyId();
        boolean inState = metadata.getKeys().containsKey(id);
        // a cache built from a newer state than the visible one is expected while that state is being applied
        assert inState == false || current.decryptedKeys().containsKey(id) || visible.version() <= current.stateVersion()
            : "key [" + id + "] is in cluster state but not cached";
        if (inState == false && missingKeyIdWarnings.add(id)) {
            logger.warn("project encryption key [{}] is not in cluster state (active key is [{}])", id, metadata.getActiveKeyId());
        }
    }

    private record KeyCache(
        @Nullable String activeKeyId,
        Map<String, SecretKey> decryptedKeys,
        @Nullable EncryptionServiceState unavailableState,
        long stateVersion
    ) {
        static final KeyCache EMPTY = new KeyCache(null, Map.of(), null, -1L);

        KeyCache {
            assert unavailableState != null || activeKeyId == null || decryptedKeys.containsKey(activeKeyId);
        }
    }
}
