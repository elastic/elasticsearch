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
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.GatewayService;

import java.util.HashMap;
import java.util.Map;

import javax.crypto.SecretKey;

/**
 * Owns the in-memory cache of the project encryption key (PEK) material and serves it to {@link AesGcmEncryptionService}.
 *
 * <p>This service listens to cluster-state changes and rebuilds its local cache whenever {@link ProjectEncryptionKeyMetadata} changes.
 * Installation, rotation, and retirement of PEK material are owned by {@link KeyRotationCoordinator}; this class is read-only with
 * respect to cluster state.
 */
public class ProjectEncryptionKeyService implements AesGcmEncryptionService.KeyProvider {

    private static final Logger logger = LogManager.getLogger(ProjectEncryptionKeyService.class);

    public static final FeatureFlag PROJECT_ENCRYPTION_KEY_FEATURE_FLAG = new FeatureFlag("project_encryption_key");

    // Prevents key generation in a mixed-version cluster. Without this, TransportVersion filtering
    // would omit the PEK custom from cluster state sent to old nodes (they lack the transport version
    // for it). If an old node becomes master, it sees no existing key and generates a new one,
    // orphaning any data encrypted with the original key.
    public static final NodeFeature PROJECT_ENCRYPTION_KEY_FEATURE = new NodeFeature("security.primary_encryption_key");

    private final ProjectResolver projectResolver;
    private volatile KeyCache cache = KeyCache.EMPTY;

    private ProjectEncryptionKeyService(ProjectResolver projectResolver) {
        this.projectResolver = projectResolver;
    }

    public static ProjectEncryptionKeyService create(ClusterService clusterService, ProjectResolver projectResolver) {
        ProjectEncryptionKeyService service = new ProjectEncryptionKeyService(projectResolver);
        clusterService.addListener(service::onClusterStateChanged);
        return service;
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
                logger.debug("project encryption key cache cleared after snapshot restore");
                this.cache = KeyCache.EMPTY;
            }
            return;
        }

        Map<String, SecretKey> keysByKeyId = HashMap.newHashMap(metadata.getKeys().size());
        for (String keyId : metadata.getKeys().keySet()) {
            SecretKey secretKey = metadata.toSecretKey(keyId);
            assert secretKey != null : "key [" + keyId + "] present in metadata but toSecretKey returned null";
            keysByKeyId.put(keyId, secretKey);
        }
        this.cache = new KeyCache(metadata.getActiveKeyId(), keysByKeyId);
        logger.debug("project encryption key cache updated: activeKeyId={}", metadata.getActiveKeyId());
    }

    @Override
    @Nullable
    public AesGcmEncryptionService.ActiveKey getActiveKey() {
        KeyCache snapshot = cache;
        if (snapshot.activeKeyId() == null) {
            return null;
        }
        return new AesGcmEncryptionService.ActiveKey(snapshot.activeKeyId(), snapshot.activeKey());
    }

    @Override
    @Nullable
    public SecretKey getKey(String keyId) {
        return cache.keysByKeyId().get(keyId);
    }

    private record KeyCache(String activeKeyId, Map<String, SecretKey> keysByKeyId) {
        static final KeyCache EMPTY = new KeyCache(null, Map.of());

        KeyCache {
            assert activeKeyId == null || keysByKeyId.containsKey(activeKeyId);
        }

        @Nullable
        SecretKey activeKey() {
            return activeKeyId != null ? keysByKeyId.get(activeKeyId) : null;
        }
    }
}
