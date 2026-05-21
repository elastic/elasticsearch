/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xpack.core.crypto.PrimaryEncryptionKeyMetadata;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.SecretKey;

/**
 * Generates and distributes the primary encryption key via cluster state.
 *
 * <p>On master election, if no primary encryption key exists in the project metadata,
 * this service generates a new AES-256 key and publishes it to cluster state.
 */
public class PrimaryEncryptionKeyService implements AesGcmEncryptionService.KeyProvider {

    private static final Logger logger = LogManager.getLogger(PrimaryEncryptionKeyService.class);

    public static final FeatureFlag PRIMARY_ENCRYPTION_KEY_FEATURE_FLAG = new FeatureFlag("primary_encryption_key");

    // Prevents key generation in a mixed-version cluster. Without this, TransportVersion filtering
    // would omit the PEK custom from cluster state sent to old nodes (they lack the transport version
    // for it). If an old node becomes master, it sees no existing key and generates a new one,
    // orphaning any data encrypted with the original key.
    public static final NodeFeature PRIMARY_ENCRYPTION_KEY_FEATURE = new NodeFeature("security.primary_encryption_key");

    private final MasterServiceTaskQueue<InstallKeyTask> taskQueue;
    private final ProjectResolver projectResolver;
    private final FeatureService featureService;
    private volatile KeyCache cache = KeyCache.EMPTY;

    private PrimaryEncryptionKeyService(
        MasterServiceTaskQueue<InstallKeyTask> taskQueue,
        ProjectResolver projectResolver,
        FeatureService featureService
    ) {
        this.taskQueue = taskQueue;
        this.projectResolver = projectResolver;
        this.featureService = featureService;
    }

    public static PrimaryEncryptionKeyService create(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        FeatureService featureService
    ) {
        PrimaryEncryptionKeyService service = new PrimaryEncryptionKeyService(
            clusterService.createTaskQueue("primary-encryption-key", Priority.NORMAL, new InstallKeyExecutor(projectResolver)),
            projectResolver,
            featureService
        );
        clusterService.addListener(service::onClusterStateChanged);
        return service;
    }

    private void onClusterStateChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();

        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            logger.debug("primary encryption key: cluster not recovered yet, skipping");
            return;
        }

        if (event.changedCustomProjectMetadataSet().contains(PrimaryEncryptionKeyMetadata.TYPE)) {
            ProjectState projectState = projectResolver.getProjectState(state);
            PrimaryEncryptionKeyMetadata metadata = projectState.metadata().custom(PrimaryEncryptionKeyMetadata.TYPE);

            if (metadata == null && this.cache != KeyCache.EMPTY) {
                logger.debug("primary encryption key cache cleared after snapshot restore");
                this.cache = KeyCache.EMPTY;
            }

            if (metadata != null) {
                Map<String, SecretKey> keysByKeyId = HashMap.newHashMap(metadata.getKeys().size());
                for (Map.Entry<String, byte[]> entry : metadata.getKeys().entrySet()) {
                    SecretKey secretKey = metadata.toSecretKey(entry.getKey());
                    assert secretKey != null : "key [" + entry.getKey() + "] present in metadata but toSecretKey returned null";
                    keysByKeyId.put(entry.getKey(), secretKey);
                }
                this.cache = new KeyCache(metadata.getActiveKeyId(), keysByKeyId);
                logger.debug("primary encryption key cache updated: activeKeyId={}", metadata.getActiveKeyId());
                return;
            }
        }

        if (event.localNodeMaster() && cache.activeKeyId == null) {
            if (featureService.clusterHasFeature(state, PRIMARY_ENCRYPTION_KEY_FEATURE) == false) {
                logger.debug("not all nodes support primary encryption key feature, waiting for rolling upgrade to complete");
                return;
            }
            logger.debug("submitting install-primary-encryption-key task");
            taskQueue.submitTask("install-primary-encryption-key", new InstallKeyTask(), null);
        }
    }

    @Override
    @Nullable
    public AesGcmEncryptionService.ActiveKey getActiveKey() {
        KeyCache snapshot = cache;
        if (snapshot.activeKeyId() == null) {
            return null;
        }
        return new AesGcmEncryptionService.ActiveKey(snapshot.activeKeyId(), snapshot.keysByKeyId().get(snapshot.activeKeyId()));
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

    private static class InstallKeyTask implements ClusterStateTaskListener {
        @Override
        public void onFailure(@Nullable Exception e) {
            logger.log(
                MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.ERROR,
                () -> "failure during primary encryption key installation",
                e
            );
        }
    }

    private static class InstallKeyExecutor extends SimpleBatchedExecutor<InstallKeyTask, Void> {
        private final ProjectResolver projectResolver;

        InstallKeyExecutor(ProjectResolver projectResolver) {
            this.projectResolver = projectResolver;
        }

        @Override
        public Tuple<ClusterState, Void> executeTask(InstallKeyTask task, ClusterState clusterState) {
            ProjectState projectState = projectResolver.getProjectState(clusterState);
            PrimaryEncryptionKeyMetadata existing = projectState.metadata().custom(PrimaryEncryptionKeyMetadata.TYPE);

            if (existing != null) {
                // Key rotation not yet supported - do nothing if a key has already been set
                return Tuple.tuple(clusterState, null);
            }

            byte[] keyBytes = new byte[32];
            new SecureRandom().nextBytes(keyBytes);
            String keyId = PrimaryEncryptionKeyMetadata.generateKeyId();
            PrimaryEncryptionKeyMetadata metadata = new PrimaryEncryptionKeyMetadata(Map.of(keyId, keyBytes), keyId);
            logger.info("installing primary encryption key [{}]", keyId);

            ClusterState newState = clusterState.copyAndUpdateProject(
                projectState.projectId(),
                b -> b.putCustom(PrimaryEncryptionKeyMetadata.TYPE, metadata)
            );
            return Tuple.tuple(newState, null);
        }

        @Override
        public void taskSucceeded(InstallKeyTask task, Void unused) {
            logger.debug("primary encryption key installed successfully");
        }

        @Override
        public String describeTasks(List<InstallKeyTask> tasks) {
            return "install primary encryption key [" + tasks.size() + " pending]";
        }
    }
}
