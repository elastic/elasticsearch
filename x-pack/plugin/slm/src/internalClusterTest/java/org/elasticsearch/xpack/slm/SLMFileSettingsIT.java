/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.admin.cluster.repositories.reservedstate.ReservedRepositoryAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyItem;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.ReservedSnapshotAction;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.elasticsearch.xcontent.XContentType.JSON;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class SLMFileSettingsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, SnapshotLifecycle.class);
    }

    private static final String REPO = "repo";

    private static AtomicLong versionCounter = new AtomicLong(1);

    private static String testJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "cluster_settings": {
                     "indices.recovery.max_bytes_per_sec": "50mb"
                 },
                 "snapshot_repositories": {
                    "repo": {
                       "type": "fs",
                       "settings": {
                          "location": "my_backup_location"
                       }
                    }
                 },
                 "slm": {
                    "test-snapshots": {
                        "schedule": "0 1 2 3 4 ?",
                        "name": "<production-snap-{now/d}>",
                        "repository": "repo",
                        "config": {
                            "indices": ["test*"],
                            "ignore_unavailable": true,
                            "include_global_state": false
                        },
                        "retention": {
                            "expire_after": "30d",
                            "min_count": 1,
                            "max_count": 50
                        }
                    }
                 }
             }
        }""";

    private static String testErrorJSON = """
        {
             "metadata": {
                 "version": "%s",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "cluster_settings": {
                     "search.allow_expensive_queries": "false"
                 },
                 "slm": {
                    "test-snapshots-err": {
                        "schedule": "* * * 31 FEB ? *",
                        "name": "<production-snap-{now/d}>",
                        "repository": "other-repo",
                        "config": {
                            "indices": ["test*"],
                            "ignore_unavailable": true,
                            "include_global_state": false
                        },
                        "retention": {
                            "expire_after": "30d",
                            "min_count": 1,
                            "max_count": 50
                        }
                    }
                 }
             }
        }""";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LifecycleSettings.SLM_HISTORY_INDEX_ENABLED, false).build();
    }

    private void writeJSONFile(String node, String json) throws Exception {
        long version = versionCounter.incrementAndGet();

        FileSettingsService fileSettingsService = internalCluster().getInstance(FileSettingsService.class, node);

        Files.createDirectories(fileSettingsService.watchedFileDir());
        Path tempFilePath = createTempFile();

        Files.writeString(tempFilePath, Strings.format(json, version));
        Files.move(tempFilePath, fileSettingsService.watchedFile(), StandardCopyOption.ATOMIC_MOVE);
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListener(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null) {
                    ReservedStateHandlerMetadata handlerMetadata = reservedState.handlers().get(ReservedClusterSettingsAction.NAME);
                    if (handlerMetadata != null && handlerMetadata.keys().contains("indices.recovery.max_bytes_per_sec")) {
                        clusterService.removeListener(this);
                        metadataVersion.set(event.state().metadata().version());
                        savedClusterState.countDown();
                    }
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertClusterStateSaveOK(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(metadataVersion.get())
        ).get();

        var reservedState = clusterStateResponse.getState().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);

        assertThat(reservedState.handlers().get(ReservedSnapshotAction.NAME).keys(), containsInAnyOrder("test-snapshots"));
        assertThat(reservedState.handlers().get(ReservedRepositoryAction.NAME).keys(), containsInAnyOrder("repo"));

        assertThat(
            clusterStateResponse.getState().metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()),
            equalTo("50mb")
        );

        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "1234kb")
        );
        assertEquals(
            "java.lang.IllegalArgumentException: Failed to process request "
                + "[org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest/unset] "
                + "with errors: [[indices.recovery.max_bytes_per_sec] set as read-only by [file_settings]]",
            expectThrows(ExecutionException.class, () -> clusterAdmin().updateSettings(req).get()).getMessage()
        );

        assertTrue(
            expectThrows(
                IllegalArgumentException.class,
                () -> client().execute(PutSnapshotLifecycleAction.INSTANCE, sampleRestRequest("test-snapshots")).actionGet()
            ).getMessage().contains("[[test-snapshots] set as read-only by [file_settings]]")
        );
    }

    public void testSettingsApplied() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        var dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));

        var savedClusterState = setupClusterStateListener(dataNode);
        // In internal cluster tests, the nodes share the config directory, so when we write with the data node path
        // the master will pick it up on start
        writeJSONFile(dataNode, testJSON);

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        awaitMasterNode(internalCluster().getNonMasterNodeName(), masterNode);

        assertClusterStateSaveOK(savedClusterState.v1(), savedClusterState.v2());

        final String indexName = "test";
        final String policyName = "test-snapshots";
        int docCount = 20;
        for (int i = 0; i < docCount; i++) {
            index(indexName, i + "", Collections.singletonMap("foo", "bar"));
        }

        logger.info("--> create snapshot manually");
        var request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "file-snap").waitForCompletion(true);
        var response = clusterAdmin().createSnapshot(request).get();
        RestStatus status = response.getSnapshotInfo().status();
        assertEquals(RestStatus.OK, status);

        logger.info("--> executing snapshot lifecycle");
        final String snapshotName = executePolicy(policyName);

        // Check that the executed snapshot shows up in the SLM output
        assertBusy(() -> {
            GetSnapshotLifecycleAction.Response getResp = client().execute(
                GetSnapshotLifecycleAction.INSTANCE,
                new GetSnapshotLifecycleAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, policyName)
            ).get();
            logger.info("--> checking for snapshot complete...");

            assertThat(getResp.getPolicies().size(), greaterThan(0));
            SnapshotLifecyclePolicyItem item = getResp.getPolicies().get(0);
            assertNotNull(item.getLastSuccess());
            var success = item.getLastSuccess();
            assertThat(success.getSnapshotStartTimestamp(), greaterThan(0L));
            assertNull(item.getLastFailure());
        });

        // Cancel/delete the snapshot
        try {
            clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, REPO, snapshotName).get();
        } catch (SnapshotMissingException e) {
            // ignore
        }
    }

    private Tuple<CountDownLatch, AtomicLong> setupClusterStateListenerForError(String node) {
        ClusterService clusterService = internalCluster().clusterService(node);
        CountDownLatch savedClusterState = new CountDownLatch(1);
        AtomicLong metadataVersion = new AtomicLong(-1);
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                ReservedStateMetadata reservedState = event.state().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);
                if (reservedState != null && reservedState.errorMetadata() != null) {
                    assertEquals(ReservedStateErrorMetadata.ErrorKind.VALIDATION, reservedState.errorMetadata().errorKind());
                    assertThat(reservedState.errorMetadata().errors(), allOf(notNullValue(), hasSize(1)));
                    assertThat(reservedState.errorMetadata().errors().get(0), containsString("no such repository [other-repo]"));
                    clusterService.removeListener(this);
                    metadataVersion.set(event.state().metadata().version());
                    savedClusterState.countDown();
                }
            }
        });

        return new Tuple<>(savedClusterState, metadataVersion);
    }

    private void assertClusterStateNotSaved(CountDownLatch savedClusterState, AtomicLong metadataVersion) throws Exception {
        boolean awaitSuccessful = savedClusterState.await(20, TimeUnit.SECONDS);
        assertTrue(awaitSuccessful);

        final ClusterStateResponse clusterStateResponse = clusterAdmin().state(
            new ClusterStateRequest(TEST_REQUEST_TIMEOUT).waitForMetadataVersion(metadataVersion.get())
        ).actionGet();

        assertThat(clusterStateResponse.getState().metadata().persistentSettings().get("search.allow_expensive_queries"), nullValue());

        var reservedState = clusterStateResponse.getState().metadata().reservedStateMetadata().get(FileSettingsService.NAMESPACE);

        assertTrue(
            reservedState.handlers().get(ReservedSnapshotAction.NAME) == null
                || reservedState.handlers().get(ReservedSnapshotAction.NAME).keys().contains("test-snapshots-err") == false
        );
        assertTrue(
            reservedState.handlers().get(ReservedRepositoryAction.NAME) == null
                || reservedState.handlers().get(ReservedRepositoryAction.NAME).keys().contains("other-repo") == false
        );
        assertTrue(
            reservedState.handlers().get(ReservedClusterSettingsAction.NAME) == null
                || reservedState.handlers()
                    .get(ReservedClusterSettingsAction.NAME)
                    .keys()
                    .contains("search.allow_expensive_queries") == false
        );

        // This should succeed, nothing was reserved
        updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", "false"));
        // This will fail because repo-new isn't there, not because we can't write test-snapshots-err, meaning we were allowed to
        // make the request
        assertEquals(
            "no such repository [repo-new]",
            expectThrows(
                IllegalArgumentException.class,
                () -> client().execute(PutSnapshotLifecycleAction.INSTANCE, sampleRestRequest("test-snapshots-err")).actionGet()
            ).getMessage()
        );
    }

    public void testErrorSaved() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        awaitMasterNode(internalCluster().getMasterName(), masterNode);
        var savedClusterState = setupClusterStateListenerForError(masterNode);

        writeJSONFile(masterNode, testErrorJSON);
        assertClusterStateNotSaved(savedClusterState.v1(), savedClusterState.v2());
    }

    private String executePolicy(String policyId) {
        ExecuteSnapshotLifecycleAction.Request executeReq = new ExecuteSnapshotLifecycleAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            policyId
        );
        ExecuteSnapshotLifecycleAction.Response resp = null;
        try {
            resp = client().execute(ExecuteSnapshotLifecycleAction.INSTANCE, executeReq).get();
            return resp.getSnapshotName();
        } catch (Exception e) {
            logger.error("failed to execute policy", e);
            fail("failed to execute policy " + policyId + " got: " + e);
            return "bad";
        }
    }

    private PutSnapshotLifecycleAction.Request sampleRestRequest(String name) throws Exception {
        var json = """
            {
                "schedule": "0 1 2 3 4 ?",
                "name": "<production-snap-{now/d}>",
                "repository": "repo-new",
                "config": {
                    "indices": ["test*"],
                    "ignore_unavailable": true,
                    "include_global_state": false
                },
                "retention": {
                    "expire_after": "30d",
                    "min_count": 1,
                    "max_count": 50
                }
            }""";

        try (
            var bis = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bis)
        ) {
            var policy = SnapshotLifecyclePolicy.parse(parser, name);
            return new PutSnapshotLifecycleAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name, policy);
        }
    }
}
