/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Owns the in-memory cache of the project encryption key (PEK) material and serves it to {@link AesGcmEncryptionService}.
 *
 * <p>This service listens to cluster-state changes and rebuilds its local encrypted-key cache whenever
 * {@link ProjectEncryptionKeyMetadata} changes. Installation, rotation, and retirement of PEK material are owned by
 * {@link KeyRotationCoordinator}; this class is read-only with respect to cluster state.
 *
 * <p>Plaintext PEKs are derived lazily on first {@link #getActiveKey}/{@link #getKey} call: the wrapped bytes are unwrapped under the
 * password identified by the metadata's {@code passwordId} (resolved from secure settings) and cached in-memory for subsequent reads.
 * On a secure-settings reload, {@link #reload(Settings)} drops the plaintext cache so the next access re-derives under whatever
 * password is now configured.
 */
class ProjectEncryptionKeyService implements AesGcmEncryptionService.KeyProvider, Closeable {

    private static final Logger logger = LogManager.getLogger(ProjectEncryptionKeyService.class);

    static final FeatureFlag PROJECT_ENCRYPTION_KEY_FEATURE_FLAG = new FeatureFlag("project_encryption_key");

    // Prevents key generation in a mixed-version cluster. Without this, TransportVersion filtering
    // would omit the PEK custom from cluster state sent to old nodes (they lack the transport version
    // for it). If an old node becomes master, it sees no existing key and generates a new one,
    // orphaning any data encrypted with the original key.
    static final NodeFeature PROJECT_ENCRYPTION_KEY_FEATURE = new NodeFeature("security.primary_encryption_key");

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final ClusterStateListener clusterStateListener = this::onClusterStateChanged;
    private volatile Settings cachedSettings;
    private volatile KeyCache cache = KeyCache.EMPTY;

    private ProjectEncryptionKeyService(ClusterService clusterService, ProjectResolver projectResolver, Settings settings) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.cachedSettings = ProjectEncryptionKeyPasswordSettings.cloneSettings(settings);
    }

    static ProjectEncryptionKeyService create(ClusterService clusterService, ProjectResolver projectResolver, Settings settings) {
        ProjectEncryptionKeyService service = new ProjectEncryptionKeyService(clusterService, projectResolver, settings);
        clusterService.addListener(service.clusterStateListener);
        return service;
    }

    @Override
    public void close() {
        clusterService.removeListener(clusterStateListener);
    }

    /**
     * Re-binds the service to the password material in {@code settings} (after a secure-settings reload). If the password id
     * the cached PEKs were unwrapped under is no longer present in the new snapshot, drops the decrypted cache so the next
     * {@link #getActiveKey}/{@link #getKey} call re-derives under whatever is now configured.
     */
    void reload(Settings settings) {
        this.cachedSettings = ProjectEncryptionKeyPasswordSettings.cloneSettings(settings);
        KeyCache snapshot = cache;
        if (snapshot == KeyCache.EMPTY || (snapshot.decryptedKeys.isEmpty() && snapshot.lockedKeyIds.isEmpty())) {
            return;
        }
        if (ProjectEncryptionKeyPasswordSettings.hasPassword(this.cachedSettings, snapshot.passwordId)) {
            if (snapshot.lockedKeyIds.isEmpty() == false) {
                cache = new KeyCache(snapshot.activeKeyId, snapshot.passwordId, snapshot.encryptedKeys, snapshot.decryptedKeys, Set.of());
                logger.debug("project encryption key locked keys cleared after secure settings reload");
            }
            return;
        }
        cache = new KeyCache(snapshot.activeKeyId, snapshot.passwordId, snapshot.encryptedKeys, Map.of(), Set.of());
        logger.debug("project encryption key cache invalidated after secure settings reload");
    }

    /**
     * Returns the active password id from the most recent snapshot of secure settings (initial settings or last
     * {@link #reload(Settings)}). May be {@code null} if no active password is configured.
     */
    @Nullable
    String getActivePasswordId() {
        return ProjectEncryptionKeyPasswordSettings.getActivePasswordId(cachedSettings);
    }

    private void onClusterStateChanged(ClusterChangedEvent event) {
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

        if (metadata == null) {
            if (this.cache != KeyCache.EMPTY) {
                logger.debug("project encryption key cache cleared after metadata removed");
                this.cache = KeyCache.EMPTY;
            }
            return;
        }

        Map<String, EncryptedData> encryptedKeys = HashMap.newHashMap(metadata.getKeys().size());
        for (String keyId : metadata.getKeys().keySet()) {
            encryptedKeys.put(keyId, metadata.getEncryptedKey(keyId));
        }
        // The passwordId may have changed, or even if it hasn't the cluster-state update may have rewrapped entries (password rotation).
        this.cache = new KeyCache(metadata.getActiveKeyId(), metadata.getPasswordId(), Map.copyOf(encryptedKeys), Map.of(), Set.of());
        logger.debug(
            "project encryption key cache updated: activeKeyId={}, passwordId={}",
            metadata.getActiveKeyId(),
            metadata.getPasswordId()
        );
    }

    @Override
    @Nullable
    public AesGcmEncryptionService.ActiveKey getActiveKey() {
        KeyCache snapshot = cache;
        if (snapshot.activeKeyId == null) {
            return null;
        }
        SecretKey key = unwrapAndCache(snapshot, snapshot.activeKeyId);
        if (key == null) {
            return null;
        }
        return new AesGcmEncryptionService.ActiveKey(snapshot.activeKeyId, key);
    }

    @Override
    @Nullable
    public SecretKey getKey(String keyId) {
        return unwrapAndCache(cache, keyId);
    }

    @Nullable
    private SecretKey unwrapAndCache(KeyCache snapshot, String keyId) {
        SecretKey decrypted = snapshot.decryptedKeys.get(keyId);
        if (decrypted != null) {
            return decrypted;
        }
        if (snapshot.lockedKeyIds.contains(keyId)) {
            // Already failed for this cache snapshot; skip PBKDF2 until the cache is reset by a reload or cluster-state change.
            return null;
        }
        EncryptedData encrypted = snapshot.encryptedKeys.get(keyId);
        if (encrypted == null) {
            return null;
        }
        try (SecureString password = ProjectEncryptionKeyPasswordSettings.getPassword(cachedSettings, snapshot.passwordId)) {
            if (password == null) {
                logger.error(
                    "project encryption key cannot be unlocked: secure setting [{}{}] is not configured",
                    ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX,
                    snapshot.passwordId
                );
                installLocked(snapshot, keyId);
                return null;
            }
            byte[] plaintext;
            try {
                plaintext = PasswordBasedEncryption.unwrap(encrypted, password.getChars());
            } catch (ElasticsearchException e) {
                logger.error(
                    () -> "project encryption key unwrap failed for keyId [" + keyId + "] under password id [" + snapshot.passwordId + "]",
                    e
                );
                installLocked(snapshot, keyId);
                return null;
            }
            try {
                SecretKey secretKey = new SecretKeySpec(plaintext, "AES");
                installDecrypted(snapshot, keyId, secretKey);
                return secretKey;
            } finally {
                Arrays.fill(plaintext, (byte) 0);
            }
        }
    }

    private synchronized void installDecrypted(KeyCache snapshotAtUnwrap, String keyId, SecretKey secretKey) {
        KeyCache current = cache;
        if (current.encryptedKeys != snapshotAtUnwrap.encryptedKeys) {
            // Cache rotated under us; a fresh cluster-state update has installed new encrypted keys and dropped the decrypted cache. The
            // next access will re-derive against the new snapshot.
            return;
        }
        if (current.decryptedKeys.containsKey(keyId)) {
            return;
        }
        Map<String, SecretKey> newDecrypted = new HashMap<>(current.decryptedKeys);
        newDecrypted.put(keyId, secretKey);
        cache = new KeyCache(
            current.activeKeyId,
            current.passwordId,
            current.encryptedKeys,
            Map.copyOf(newDecrypted),
            current.lockedKeyIds
        );
    }

    private synchronized void installLocked(KeyCache snapshotAtUnwrap, String keyId) {
        KeyCache current = cache;
        if (current.encryptedKeys != snapshotAtUnwrap.encryptedKeys) {
            return;
        }
        if (current.lockedKeyIds.contains(keyId)) {
            return;
        }
        Set<String> newLocked = new HashSet<>(current.lockedKeyIds);
        newLocked.add(keyId);
        cache = new KeyCache(current.activeKeyId, current.passwordId, current.encryptedKeys, current.decryptedKeys, Set.copyOf(newLocked));
    }

    private record KeyCache(
        @Nullable String activeKeyId,
        @Nullable String passwordId,
        Map<String, EncryptedData> encryptedKeys,
        Map<String, SecretKey> decryptedKeys,
        Set<String> lockedKeyIds
    ) {
        static final KeyCache EMPTY = new KeyCache(null, null, Map.of(), Map.of(), Set.of());

        KeyCache {
            assert activeKeyId == null || encryptedKeys.containsKey(activeKeyId);
        }
    }
}
