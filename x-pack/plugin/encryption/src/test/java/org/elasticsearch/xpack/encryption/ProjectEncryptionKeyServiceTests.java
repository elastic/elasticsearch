/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ProjectEncryptionKeyServiceTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("test");

    private static ClusterStateListener captureListener(ClusterService clusterService) {
        ArgumentCaptor<ClusterStateListener> captor = ArgumentCaptor.forClass(ClusterStateListener.class);
        verify(clusterService).addListener(captor.capture());
        return captor.getValue();
    }

    private static ProjectEncryptionKeyMetadata randomPekMetadata() {
        byte[] keyBytes = new byte[32];
        random().nextBytes(keyBytes);
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        return new ProjectEncryptionKeyMetadata(Map.of(keyId, new ProjectEncryptionKeyMetadata.KeyEntry(keyBytes, 0L)), keyId);
    }

    private static DiscoveryNodes nodes() {
        DiscoveryNode localNode = DiscoveryNodeUtils.create("local");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder().add(localNode).localNodeId("local").masterNodeId("local");
        return builder.build();
    }

    private static ClusterState stateWithKey(ProjectEncryptionKeyMetadata pek) {
        return stateWithKeyOnProject(Metadata.DEFAULT_PROJECT_ID, pek);
    }

    private static ClusterState stateWithKeyOnProject(ProjectId projectId, ProjectEncryptionKeyMetadata pek) {
        ProjectMetadata project = ProjectMetadata.builder(projectId).putCustom(ProjectEncryptionKeyMetadata.TYPE, pek).build();
        return ClusterState.builder(CLUSTER_NAME).metadata(Metadata.builder().put(project).build()).nodes(nodes()).build();
    }

    private static ClusterState stateWithoutKey() {
        return ClusterState.builder(CLUSTER_NAME).nodes(nodes()).build();
    }

    /** Builds cluster state with each project from {@code pekByProject} keyed by its (possibly null) metadata. */
    private static ClusterState stateWithProjects(Map<ProjectId, ProjectEncryptionKeyMetadata> pekByProject) {
        Metadata.Builder metadata = Metadata.builder();
        for (var entry : pekByProject.entrySet()) {
            ProjectMetadata.Builder project = ProjectMetadata.builder(entry.getKey());
            if (entry.getValue() != null) {
                project.putCustom(ProjectEncryptionKeyMetadata.TYPE, entry.getValue());
            }
            metadata.put(project.build());
        }
        return ClusterState.builder(CLUSTER_NAME).metadata(metadata.build()).nodes(nodes()).build();
    }

    public void testGetActiveKeyReturnsNullInitially() {
        ClusterService clusterService = mock(ClusterService.class);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE);
        assertNull(service.getActiveKey());
        assertNull(service.getKey("anything"));
    }

    public void testServiceRegistersAsClusterStateListener() {
        ClusterService clusterService = mock(ClusterService.class);
        ProjectEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE);
        captureListener(clusterService);
    }

    public void testCacheUpdatedOnMetadataChange() {
        ClusterService clusterService = mock(ClusterService.class);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE);
        ClusterStateListener listener = captureListener(clusterService);

        ProjectEncryptionKeyMetadata pek = randomPekMetadata();
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithKey(pek), stateWithoutKey()));

        AesGcmEncryptionService.ActiveKey activeKey = service.getActiveKey();
        assertNotNull(activeKey);
        assertEquals(pek.getActiveKeyId(), activeKey.keyId());
        assertEquals("AES", activeKey.key().getAlgorithm());
        assertNotNull(service.getKey(pek.getActiveKeyId()));
        assertNull(service.getKey("nonexistent"));
    }

    public void testGatewayBlockSkipsCacheUpdate() {
        ClusterService clusterService = mock(ClusterService.class);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE);
        ClusterStateListener listener = captureListener(clusterService);

        ClusterState blockedState = ClusterState.builder(stateWithKey(randomPekMetadata()))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        listener.clusterChanged(new ClusterChangedEvent("test", blockedState, stateWithoutKey()));

        assertNull(service.getActiveKey());
    }

    public void testCacheClearedWhenMetadataRemoved() {
        ClusterService clusterService = mock(ClusterService.class);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(clusterService, DefaultProjectResolver.INSTANCE);
        ClusterStateListener listener = captureListener(clusterService);

        ProjectEncryptionKeyMetadata pek = randomPekMetadata();
        ClusterState withKey = stateWithKey(pek);
        listener.clusterChanged(new ClusterChangedEvent("test", withKey, stateWithoutKey()));
        assertNotNull(service.getActiveKey());

        // Replace the project's PEK metadata with absence — same project, but no PEK custom anymore.
        ProjectMetadata emptyProject = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID).build();
        ClusterState withoutKeyOnDefaultProject = ClusterState.builder(CLUSTER_NAME)
            .metadata(Metadata.builder().put(emptyProject).build())
            .nodes(nodes())
            .build();
        listener.clusterChanged(new ClusterChangedEvent("test", withoutKeyOnDefaultProject, withKey));
        assertNull(service.getActiveKey());
    }

    public void testTwoProjectsKeepDistinctKeys() {
        // Regression for elastic/elasticsearch#149194: a single-slot cache silently dropped one project's keys when another project's
        // metadata changed. With a project-keyed cache, each project's PEK must be reachable independently when the caller's
        // ThreadContext names that project.
        ClusterService clusterService = mock(ClusterService.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ProjectResolver projectResolver = TestProjectResolvers.usingRequestHeader(threadContext);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(clusterService, projectResolver);
        ClusterStateListener listener = captureListener(clusterService);

        ProjectId p1 = ProjectId.fromId("p1");
        ProjectId p2 = ProjectId.fromId("p2");
        ProjectEncryptionKeyMetadata pek1 = randomPekMetadata();
        ProjectEncryptionKeyMetadata pek2 = randomPekMetadata();
        Map<ProjectId, ProjectEncryptionKeyMetadata> projects = new HashMap<>();
        projects.put(p1, pek1);
        projects.put(p2, pek2);
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithProjects(projects), stateWithoutKey()));

        projectResolver.executeOnProject(p1, () -> {
            AesGcmEncryptionService.ActiveKey activeKey = service.getActiveKey();
            assertNotNull("p1 must see its own key", activeKey);
            assertEquals(pek1.getActiveKeyId(), activeKey.keyId());
        });

        projectResolver.executeOnProject(p2, () -> {
            AesGcmEncryptionService.ActiveKey activeKey = service.getActiveKey();
            assertNotNull("p2 must see its own key", activeKey);
            assertEquals(pek2.getActiveKeyId(), activeKey.keyId());
        });
    }

    public void testProjectAdditionAndRemoval() {
        ClusterService clusterService = mock(ClusterService.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ProjectResolver projectResolver = TestProjectResolvers.usingRequestHeader(threadContext);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(clusterService, projectResolver);
        ClusterStateListener listener = captureListener(clusterService);

        ProjectId p1 = ProjectId.fromId("p1");
        ProjectId p2 = ProjectId.fromId("p2");
        ProjectEncryptionKeyMetadata pek1 = randomPekMetadata();
        ProjectEncryptionKeyMetadata pek2 = randomPekMetadata();

        ClusterState withP1 = stateWithProjects(Map.of(p1, pek1));
        listener.clusterChanged(new ClusterChangedEvent("test", withP1, stateWithoutKey()));
        projectResolver.executeOnProject(p1, () -> assertNotNull(service.getActiveKey()));

        // Add p2 — p1 must remain reachable.
        Map<ProjectId, ProjectEncryptionKeyMetadata> withBoth = new HashMap<>();
        withBoth.put(p1, pek1);
        withBoth.put(p2, pek2);
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithProjects(withBoth), withP1));
        projectResolver.executeOnProject(p1, () -> assertEquals(pek1.getActiveKeyId(), service.getActiveKey().keyId()));
        projectResolver.executeOnProject(p2, () -> assertEquals(pek2.getActiveKeyId(), service.getActiveKey().keyId()));

        // Remove p1 — only p2 remains.
        ClusterState onlyP2 = stateWithProjects(Map.of(p2, pek2));
        listener.clusterChanged(new ClusterChangedEvent("test", onlyP2, stateWithProjects(withBoth)));
        projectResolver.executeOnProject(p1, () -> assertNull("p1's slot must be cleared after removal", service.getActiveKey()));
        projectResolver.executeOnProject(p2, () -> assertNotNull("p2 still reachable after p1 removal", service.getActiveKey()));
    }

    public void testMetadataChangeForOneProjectDoesNotDisturbOthers() {
        ClusterService clusterService = mock(ClusterService.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ProjectResolver projectResolver = TestProjectResolvers.usingRequestHeader(threadContext);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(clusterService, projectResolver);
        ClusterStateListener listener = captureListener(clusterService);

        ProjectId p1 = ProjectId.fromId("p1");
        ProjectId p2 = ProjectId.fromId("p2");
        ProjectEncryptionKeyMetadata pek1Initial = randomPekMetadata();
        ProjectEncryptionKeyMetadata pek2Initial = randomPekMetadata();
        Map<ProjectId, ProjectEncryptionKeyMetadata> initial = new HashMap<>();
        initial.put(p1, pek1Initial);
        initial.put(p2, pek2Initial);
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithProjects(initial), stateWithoutKey()));

        // Rotate p1's key only.
        ProjectEncryptionKeyMetadata pek1Rotated = randomPekMetadata();
        Map<ProjectId, ProjectEncryptionKeyMetadata> rotated = new HashMap<>();
        rotated.put(p1, pek1Rotated);
        rotated.put(p2, pek2Initial);
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithProjects(rotated), stateWithProjects(initial)));

        projectResolver.executeOnProject(
            p1,
            () -> assertEquals("p1's slot reflects the rotation", pek1Rotated.getActiveKeyId(), service.getActiveKey().keyId())
        );
        projectResolver.executeOnProject(
            p2,
            () -> assertEquals(
                "p2's slot must be untouched by p1's metadata change",
                pek2Initial.getActiveKeyId(),
                service.getActiveKey().keyId()
            )
        );
    }

    public void testGetActiveKeyWithoutProjectContextReturnsNullOnUnknownProject() {
        // In a multi-project cluster, a caller that forgets to set its project header lands on whatever the resolver's fallback returns
        // (typically DEFAULT_PROJECT_ID). If no PEK is installed on that fallback project, we must return null — never silently route
        // to another project's keys.
        ClusterService clusterService = mock(ClusterService.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ProjectResolver projectResolver = TestProjectResolvers.usingRequestHeader(threadContext);
        ProjectEncryptionKeyService service = ProjectEncryptionKeyService.create(clusterService, projectResolver);
        ClusterStateListener listener = captureListener(clusterService);

        ProjectId installedProject = ProjectId.fromId("only-p1");
        ProjectEncryptionKeyMetadata pek = randomPekMetadata();
        listener.clusterChanged(new ClusterChangedEvent("test", stateWithProjects(Map.of(installedProject, pek)), stateWithoutKey()));

        // No project header set — the test resolver falls back to DEFAULT_PROJECT_ID, which has no cache slot.
        assertNull("no slot for fallback project must surface as null", service.getActiveKey());
        assertNull(service.getKey(pek.getActiveKeyId()));

        // With the correct project header, the keys become reachable.
        projectResolver.executeOnProject(installedProject, () -> assertNotNull(service.getActiveKey()));
    }
}
