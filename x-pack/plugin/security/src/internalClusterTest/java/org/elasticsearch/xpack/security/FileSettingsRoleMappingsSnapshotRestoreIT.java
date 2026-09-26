/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.integration.RoleMappingFileSettingsIT;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;
import org.junit.Before;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata.METADATA_NAME_FIELD;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
@LuceneTestCase.SuppressFileSystems("*")
public class FileSettingsRoleMappingsSnapshotRestoreIT extends SecurityIntegTestCase {

    private static final int MAX_WAIT_TIME_SECONDS = 20;
    private static final String REPOSITORY_NAME = "role-mappings-repo";
    private static final String SNAPSHOT_NAME = "role-mappings-snap";
    private static final String SNAPSHOT_MAPPING_NAME = "everyone_snapshot";
    private static final String SNAPSHOT_MAPPING_ROLE = "monitoring_user";

    private final AtomicLong versionCounter = new AtomicLong(1);

    private static final String testJSONRoleMappings = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "role_mappings": {
                       "everyone_kibana": {
                          "enabled": true,
                          "roles": [ "kibana_user" ],
                          "rules": { "field": { "username": "*" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7"
                          }
                       },
                       "everyone_fleet": {
                          "enabled": true,
                          "roles": [ "fleet_user" ],
                          "rules": { "field": { "username": "*" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be3-bb8d-02bb270cb3a7"
                          }
                       }
                 }
             }
        }""";

    @Before
    public void resetVersion() {
        versionCounter.set(1);
    }

    public void testRoleMappingsAreNotIncludedInSnapshots() throws Exception {
        final String masterNode = startMasterAndDataNode();
        applyFileBasedRoleMappings(masterNode);
        assertRoleMappingsInClusterState("everyone_kibana", "everyone_fleet");

        createRepository(randomRepoPath());
        final SnapshotInfo snapshotInfo = createSnapshotWithGlobalState();

        final Metadata snapshotMetadata = readSnapshotGlobalMetadata(snapshotInfo.snapshotId());
        assertThat(snapshotMetadata.getProject(ProjectId.DEFAULT).custom(RoleMappingMetadata.TYPE), nullValue());
    }

    public void testRoleMappingsInOlderSnapshotsAreNotRestored() throws Exception {
        startMasterAndDataNode();
        assertNoRoleMappingsInClusterState();

        final Path repoPath = randomRepoPath();
        createRepository(repoPath);
        final SnapshotInfo snapshotInfo = createSnapshotWithGlobalState();
        writeRoleMappingsIntoSnapshotGlobalMetadata(repoPath, snapshotInfo.snapshotId());
        assertSnapshotContainsRoleMappings(snapshotInfo.snapshotId());

        restoreSnapshotWithGlobalState();

        assertNoRoleMappingsInClusterState();
        assertNoResolvedRoles();
    }

    public void testGlobalStateRestoreKeepsFileBasedRoleMappings() throws Exception {
        final String masterNode = startMasterAndDataNode();
        applyFileBasedRoleMappings(masterNode);

        final Path repoPath = randomRepoPath();
        createRepository(repoPath);
        final SnapshotInfo snapshotInfo = createSnapshotWithGlobalState();
        writeRoleMappingsIntoSnapshotGlobalMetadata(repoPath, snapshotInfo.snapshotId());

        restoreSnapshotWithGlobalState();

        assertRoleMappingsInClusterState("everyone_kibana", "everyone_fleet");
        assertResolvedRoles("kibana_user", "fleet_user");
    }

    private String startMasterAndDataNode() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        awaitMasterNode();
        ensureGreen();
        return masterNode;
    }

    private void applyFileBasedRoleMappings(String masterNode) throws Exception {
        awaitFileSettingsWatcher();
        final Tuple<CountDownLatch, AtomicLong> savedClusterState = RoleMappingFileSettingsIT.setupClusterStateListener(
            masterNode,
            "everyone_kibana"
        );
        RoleMappingFileSettingsIT.writeJSONFile(masterNode, testJSONRoleMappings, logger, versionCounter.incrementAndGet());
        assertTrue(savedClusterState.v1().await(MAX_WAIT_TIME_SECONDS, TimeUnit.SECONDS));
        clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(savedClusterState.v2().get()))
            .actionGet();
    }

    private void awaitFileSettingsWatcher() throws Exception {
        final FileSettingsService fileSettingsService = internalCluster().getInstance(
            FileSettingsService.class,
            internalCluster().getMasterName()
        );
        assertBusy(() -> assertTrue(fileSettingsService.watching()));
    }

    private void createRepository(Path location) {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPOSITORY_NAME)
                .setType("fs")
                .setSettings(Settings.builder().put("location", location).put("compress", false))
        );
    }

    private SnapshotInfo createSnapshotWithGlobalState() {
        final SnapshotInfo snapshotInfo = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setIncludeGlobalState(true)
            .get()
            .getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        return snapshotInfo;
    }

    private void restoreSnapshotWithGlobalState() {
        final var response = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .setFeatureStates("none")
            .get();
        assertThat(response.getRestoreInfo().failedShards(), equalTo(0));
    }

    private Metadata readSnapshotGlobalMetadata(SnapshotId snapshotId) {
        final Repository repository = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(REPOSITORY_NAME);
        return repository.getSnapshotGlobalMetadata(snapshotId, false);
    }

    private void writeRoleMappingsIntoSnapshotGlobalMetadata(Path repoPath, SnapshotId snapshotId) throws Exception {
        final RoleMappingMetadata roleMappings = new RoleMappingMetadata(
            Set.of(
                new ExpressionRoleMapping(
                    SNAPSHOT_MAPPING_NAME,
                    new FieldExpression("username", List.of(new FieldExpression.FieldValue("*"))),
                    List.of(SNAPSHOT_MAPPING_ROLE),
                    List.of(),
                    Map.of(METADATA_NAME_FIELD, SNAPSHOT_MAPPING_NAME),
                    true
                )
            )
        );
        final Metadata snapshotMetadata = readSnapshotGlobalMetadata(snapshotId);
        final ProjectMetadata project = roleMappings.updateProject(snapshotMetadata.getProject(ProjectId.DEFAULT));
        final Metadata withRoleMappings = Metadata.builder(snapshotMetadata).put(project).build();

        final String blobName = BlobStoreRepository.GLOBAL_METADATA_FORMAT.blobName(snapshotId.getUUID());
        try (OutputStream outputStream = Files.newOutputStream(repoPath.resolve(blobName))) {
            BlobStoreRepository.GLOBAL_METADATA_FORMAT.serialize(
                withRoleMappings,
                blobName,
                false,
                Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY),
                outputStream
            );
        }
    }

    private void assertSnapshotContainsRoleMappings(SnapshotId snapshotId) {
        final RoleMappingMetadata roleMappings = RoleMappingMetadata.getFromProject(
            readSnapshotGlobalMetadata(snapshotId).getProject(ProjectId.DEFAULT)
        );
        assertThat(roleMappings, notNullValue());
        assertThat(
            roleMappings.getRoleMappings().stream().map(ExpressionRoleMapping::getName).toList(),
            containsInAnyOrder(SNAPSHOT_MAPPING_NAME)
        );
    }

    private void assertRoleMappingsInClusterState(String... expectedNames) {
        assertThat(clusterStateRoleMappingNames(), containsInAnyOrder(expectedNames));
    }

    private void assertNoRoleMappingsInClusterState() {
        assertThat(clusterStateRoleMappingNames(), empty());
    }

    private List<String> clusterStateRoleMappingNames() {
        final ClusterState clusterState = clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)).actionGet().getState();
        return RoleMappingMetadata.getFromProject(clusterState.metadata().getProject(ProjectId.DEFAULT))
            .getRoleMappings()
            .stream()
            .map(ExpressionRoleMapping::getName)
            .toList();
    }

    private void assertResolvedRoles(String... expectedRoles) throws Exception {
        for (Set<String> resolvedRoles : resolveRolesOnAllNodes()) {
            assertThat(resolvedRoles, containsInAnyOrder(expectedRoles));
        }
    }

    private void assertNoResolvedRoles() throws Exception {
        for (Set<String> resolvedRoles : resolveRolesOnAllNodes()) {
            assertThat(resolvedRoles, empty());
        }
    }

    private List<Set<String>> resolveRolesOnAllNodes() throws Exception {
        final List<Set<String>> resolvedRoles = new ArrayList<>();
        for (UserRoleMapper userRoleMapper : internalCluster().getInstances(UserRoleMapper.class)) {
            final PlainActionFuture<Set<String>> future = new PlainActionFuture<>();
            userRoleMapper.resolveRoles(
                new UserRoleMapper.UserData("anyUsername", null, List.of(), Map.of(), mock(RealmConfig.class)),
                future
            );
            resolvedRoles.add(future.get());
        }
        return resolvedRoles;
    }
}
