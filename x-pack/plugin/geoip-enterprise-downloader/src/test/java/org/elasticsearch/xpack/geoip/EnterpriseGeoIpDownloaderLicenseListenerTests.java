/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.geoip;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.node.Node;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.RemovePersistentTaskAction;
import org.elasticsearch.persistent.StartPersistentTaskAction;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.xpack.geoip.EnterpriseGeoIpDownloaderLicenseListener.INGEST_GEOIP_CUSTOM_METADATA_TYPE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EnterpriseGeoIpDownloaderLicenseListenerTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new ThreadPool(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(), MeterRegistry.NOOP);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testAllConditionsMetOnStart() {
        // Should never start if not master node, even if all other conditions have been met
        final XPackLicenseState licenseState = getAlwaysValidLicense();
        ClusterService clusterService = createClusterService(true, false);
        TaskStartAndRemoveMockClient client = new TaskStartAndRemoveMockClient(threadPool, true, false);
        EnterpriseGeoIpDownloaderLicenseListener listener = new EnterpriseGeoIpDownloaderLicenseListener(
            client,
            clusterService,
            threadPool,
            licenseState
        );
        listener.init();
        listener.licenseStateChanged();
        listener.clusterChanged(new ClusterChangedEvent("test", createClusterState(true, true), clusterService.state()));
        client.assertTaskStartHasBeenCalled();
    }

    public void testLicenseChanges() {
        final TestUtils.UpdatableLicenseState licenseState = new TestUtils.UpdatableLicenseState();
        licenseState.update(new XPackLicenseStatus(License.OperationMode.TRIAL, false, ""));
        ClusterService clusterService = createClusterService(true, true);
        TaskStartAndRemoveMockClient client = new TaskStartAndRemoveMockClient(threadPool, false, true);
        EnterpriseGeoIpDownloaderLicenseListener listener = new EnterpriseGeoIpDownloaderLicenseListener(
            client,
            clusterService,
            threadPool,
            licenseState
        );
        listener.init();
        listener.licenseStateChanged();
        listener.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), clusterService.state()));
        client.expectStartTask = true;
        client.expectRemoveTask = false;
        licenseState.update(new XPackLicenseStatus(License.OperationMode.TRIAL, true, ""));
        listener.licenseStateChanged();
        client.assertTaskStartHasBeenCalled();
        client.expectStartTask = false;
        client.expectRemoveTask = true;
        licenseState.update(new XPackLicenseStatus(License.OperationMode.TRIAL, false, ""));
        listener.licenseStateChanged();
        client.assertTaskRemoveHasBeenCalled();
    }

    public void testDatabaseChanges() {
        final XPackLicenseState licenseState = getAlwaysValidLicense();
        ClusterService clusterService = createClusterService(true, false);
        TaskStartAndRemoveMockClient client = new TaskStartAndRemoveMockClient(threadPool, false, false);
        EnterpriseGeoIpDownloaderLicenseListener listener = new EnterpriseGeoIpDownloaderLicenseListener(
            client,
            clusterService,
            threadPool,
            licenseState
        );
        listener.init();
        listener.licenseStateChanged();
        listener.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), clusterService.state()));
        // add a geoip database, so the task ought to be started:
        client.expectStartTask = true;
        listener.clusterChanged(new ClusterChangedEvent("test", createClusterState(true, true), clusterService.state()));
        client.assertTaskStartHasBeenCalled();
        // Now we remove the geoip databases. The task ought to just be left alone.
        client.expectStartTask = false;
        client.expectRemoveTask = false;
        listener.clusterChanged(new ClusterChangedEvent("test", createClusterState(true, false), clusterService.state()));
    }

    public void testMasterChanges() {
        // Should never start if not master node, even if all other conditions have been met
        final XPackLicenseState licenseState = getAlwaysValidLicense();
        ClusterService clusterService = createClusterService(false, false);
        TaskStartAndRemoveMockClient client = new TaskStartAndRemoveMockClient(threadPool, false, false);
        EnterpriseGeoIpDownloaderLicenseListener listener = new EnterpriseGeoIpDownloaderLicenseListener(
            client,
            clusterService,
            threadPool,
            licenseState
        );
        listener.init();
        listener.licenseStateChanged();
        listener.clusterChanged(new ClusterChangedEvent("test", createClusterState(false, true), clusterService.state()));
        client.expectStartTask = true;
        listener.clusterChanged(new ClusterChangedEvent("test", createClusterState(true, true), clusterService.state()));
    }

    private XPackLicenseState getAlwaysValidLicense() {
        return new XPackLicenseState(() -> 0);
    }

    private ClusterService createClusterService(boolean isMasterNode, boolean hasGeoIpDatabases) {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = createClusterState(isMasterNode, hasGeoIpDatabases);
        when(clusterService.state()).thenReturn(state);
        return clusterService;
    }

    private ClusterState createClusterState(boolean isMasterNode, boolean hasGeoIpDatabases) {
        String indexName = randomAlphaOfLength(5);
        Index index = new Index(indexName, UUID.randomUUID().toString());
        IndexMetadata.Builder idxMeta = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), 1, 0).put("index.uuid", index.getUUID()));
        String nodeId = ESTestCase.randomAlphaOfLength(8);
        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(nodeId)).localNodeId(nodeId);
        if (isMasterNode) {
            discoveryNodesBuilder.masterNodeId(nodeId);
        }
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(new ClusterName("name"));
        if (hasGeoIpDatabases) {
            PersistentTasksCustomMetadata tasksCustomMetadata = new PersistentTasksCustomMetadata(1L, Map.of());
            clusterStateBuilder.metadata(Metadata.builder().putCustom(INGEST_GEOIP_CUSTOM_METADATA_TYPE, tasksCustomMetadata).put(idxMeta));
        }
        return clusterStateBuilder.nodes(discoveryNodesBuilder).build();
    }

    private static class TaskStartAndRemoveMockClient extends NoOpClient {

        boolean expectStartTask;
        boolean expectRemoveTask;
        private boolean taskStartCalled = false;
        private boolean taskRemoveCalled = false;

        private TaskStartAndRemoveMockClient(ThreadPool threadPool, boolean expectStartTask, boolean expectRemoveTask) {
            super(threadPool);
            this.expectStartTask = expectStartTask;
            this.expectRemoveTask = expectRemoveTask;
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (action.equals(StartPersistentTaskAction.INSTANCE)) {
                if (expectStartTask) {
                    taskStartCalled = true;
                } else {
                    fail("Should not start task");
                }
            } else if (action.equals(RemovePersistentTaskAction.INSTANCE)) {
                if (expectRemoveTask) {
                    taskRemoveCalled = true;
                } else {
                    fail("Should not remove task");
                }
            } else {
                throw new IllegalStateException("unexpected action called [" + action.name() + "]");
            }
        }

        void assertTaskStartHasBeenCalled() {
            assertTrue(taskStartCalled);
        }

        void assertTaskRemoveHasBeenCalled() {
            assertTrue(taskRemoveCalled);
        }
    }
}
