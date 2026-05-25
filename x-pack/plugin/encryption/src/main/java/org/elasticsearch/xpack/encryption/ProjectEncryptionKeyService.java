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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.gateway.GatewayService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.SecretKey;

/**
 * Owns the in-memory cache of the project encryption key (PEK) material and serves it to {@link AesGcmEncryptionService}.
 *
 * <p>The cache is keyed by {@link ProjectId} — one slot per project. On every cluster-state change the service iterates all projects
 * whose {@link ProjectEncryptionKeyMetadata} actually changed (using {@link ClusterChangedEvent#customMetadataChanged(ProjectId, String)})
 * and rebuilds only the affected slots. Cache slots for deleted projects are removed.
 *
 * <p>Read access through the {@link AesGcmEncryptionService.KeyProvider} accessors resolves the caller's project via the injected
 * {@link ProjectResolver}: callers (e.g. ES|QL) must already operate inside a project context (set explicitly via
 * {@link ProjectResolver#executeOnProject(ProjectId, org.elasticsearch.core.CheckedRunnable)} or implicitly via the request thread
 * context). If no slot exists for the resolved project id the accessors return {@code null}, which the encryption service surfaces as
 * an {@link org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException}. We never fall back to another project's slot.
 *
 * <p>Installation, rotation, and retirement of PEK material are owned by {@link KeyRotationCoordinator}; this class is read-only with
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
    private final ConcurrentHashMap<ProjectId, KeyCache> cacheByProject = new ConcurrentHashMap<>();

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

        // Refresh each project whose PEK metadata actually changed in this event. We do not rely on a single "current" project
        // resolved from the cluster-applier thread's (empty) ThreadContext — that's the bug class fixed by this listener.
        Set<ProjectId> projectsToCheck = new HashSet<>(state.metadata().projects().keySet());
        projectsToCheck.addAll(event.previousState().metadata().projects().keySet());

        for (ProjectId projectId : projectsToCheck) {
            if (event.customMetadataChanged(projectId, ProjectEncryptionKeyMetadata.TYPE) == false) {
                continue;
            }
            ProjectMetadata project = state.metadata().projects().get(projectId);
            ProjectEncryptionKeyMetadata metadata = project != null ? project.custom(ProjectEncryptionKeyMetadata.TYPE) : null;

            if (metadata == null) {
                if (cacheByProject.remove(projectId) != null) {
                    logger.debug("project encryption key cache cleared for project [{}]", projectId);
                }
                continue;
            }

            Map<String, SecretKey> keysByKeyId = HashMap.newHashMap(metadata.getKeys().size());
            for (String keyId : metadata.getKeys().keySet()) {
                SecretKey secretKey = metadata.toSecretKey(keyId);
                assert secretKey != null : "key [" + keyId + "] present in metadata but toSecretKey returned null";
                keysByKeyId.put(keyId, secretKey);
            }
            cacheByProject.put(projectId, new KeyCache(metadata.getActiveKeyId(), keysByKeyId));
            logger.debug("project encryption key cache updated for project [{}]: activeKeyId={}", projectId, metadata.getActiveKeyId());
        }
    }

    @Override
    @Nullable
    public AesGcmEncryptionService.ActiveKey getActiveKey() {
        ProjectId projectId = projectResolver.getProjectId();
        assert projectId != null : "ProjectResolver returned null project id";
        KeyCache slot = cacheByProject.get(projectId);
        if (slot == null || slot.activeKeyId() == null) {
            logger.debug("no encryption key available for project [{}]", projectId);
            return null;
        }
        return new AesGcmEncryptionService.ActiveKey(slot.activeKeyId(), slot.activeKey());
    }

    @Override
    @Nullable
    public SecretKey getKey(String keyId) {
        ProjectId projectId = projectResolver.getProjectId();
        assert projectId != null : "ProjectResolver returned null project id";
        KeyCache slot = cacheByProject.get(projectId);
        if (slot == null) {
            logger.debug("no encryption key cache slot for project [{}]", projectId);
            return null;
        }
        return slot.keysByKeyId().get(keyId);
    }

    private record KeyCache(String activeKeyId, Map<String, SecretKey> keysByKeyId) {

        KeyCache {
            assert activeKeyId == null || keysByKeyId.containsKey(activeKeyId);
        }

        @Nullable
        SecretKey activeKey() {
            return activeKeyId != null ? keysByKeyId.get(activeKeyId) : null;
        }
    }
}
