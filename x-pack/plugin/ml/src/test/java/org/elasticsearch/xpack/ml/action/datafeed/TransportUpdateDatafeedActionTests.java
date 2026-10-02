/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action.datafeed;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MachineLearningExtension;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportUpdateDatafeedActionTests extends ESTestCase {

    public void testMixedVersionClusterShouldRejectEsqlQueryUpdate() {
        DatafeedUpdate update = new DatafeedUpdate.Builder("datafeed-1").setEsqlQuery("FROM logs").build();

        assertTrue(
            TransportUpdateDatafeedAction.checkClusterSupportsDatafeedUpdate(
                update,
                clusterStateWithMinTransportVersion(transportVersionBeforeEsqlDatafeeds())
            ).isPresent()
        );
    }

    public void testMixedVersionClusterShouldAllowClassicUpdate() {
        DatafeedUpdate update = new DatafeedUpdate.Builder("datafeed-1").setQueryDelay(org.elasticsearch.core.TimeValue.timeValueMinutes(1))
            .build();

        assertTrue(
            TransportUpdateDatafeedAction.checkClusterSupportsDatafeedUpdate(
                update,
                clusterStateWithMinTransportVersion(transportVersionBeforeEsqlDatafeeds())
            ).isEmpty()
        );
    }

    public void testMixedVersionClusterShouldRejectEsqlQueryUpdateBeforeCallingManager() {
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        TransportUpdateDatafeedAction action = createAction(datafeedConfigProvider, jobConfigProvider, client);
        DatafeedUpdate update = new DatafeedUpdate.Builder("datafeed-1").setEsqlQuery("FROM logs").build();
        AtomicReference<Exception> failure = new AtomicReference<>();

        try (UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(update)) {
            action.masterOperation(
                null,
                request,
                clusterStateWithMinTransportVersion(transportVersionBeforeEsqlDatafeeds()),
                ActionTestUtils.assertNoSuccessListener(failure::set)
            );
        }

        assertThat(failure.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure.get()).status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(failure.get().getMessage(), containsString("cluster upgrade is in progress"));
        assertThat(failure.get().getMessage(), containsString("ES|QL datafeed updates"));
        verifyNoInteractions(datafeedConfigProvider, jobConfigProvider, client);
    }

    public void testCoordinatingNodeShouldRejectEsqlQueryUpdateBeforeSendingToOlderMaster() {
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        TransportService transportService = mock(TransportService.class);
        TransportUpdateDatafeedAction action = createAction(
            datafeedConfigProvider,
            jobConfigProvider,
            client,
            clusterService,
            transportService
        );
        when(clusterService.state()).thenReturn(coordinatingStateWithOlderMaster());
        clearInvocations(transportService);

        DatafeedUpdate update = new DatafeedUpdate.Builder("datafeed-1").setEsqlQuery("FROM logs").build();
        AtomicReference<Exception> failure = new AtomicReference<>();
        action.doExecute(null, new UpdateDatafeedAction.Request(update), ActionTestUtils.assertNoSuccessListener(failure::set));

        assertThat(failure.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure.get()).status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(failure.get().getMessage(), containsString("cluster upgrade is in progress"));
        assertThat(failure.get().getMessage(), containsString("wait for the cluster to finish upgrading"));
        verifyNoInteractions(transportService, datafeedConfigProvider, jobConfigProvider, client);
    }

    private static TransportUpdateDatafeedAction createAction(
        DatafeedConfigProvider datafeedConfigProvider,
        JobConfigProvider jobConfigProvider,
        Client client
    ) {
        return createAction(datafeedConfigProvider, jobConfigProvider, client, mock(ClusterService.class), mock(TransportService.class));
    }

    private static TransportUpdateDatafeedAction createAction(
        DatafeedConfigProvider datafeedConfigProvider,
        JobConfigProvider jobConfigProvider,
        Client client,
        ClusterService clusterService,
        TransportService transportService
    ) {
        Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Set.of(MachineLearning.REQUIRE_ROLLBACK_SNAPSHOT_BEFORE_SCOPE_CHANGE))
        );
        ProjectResolver projectResolver = mock(ProjectResolver.class);
        when(projectResolver.getProjectId()).thenReturn(ProjectId.DEFAULT);
        return new TransportUpdateDatafeedAction(
            settings,
            transportService,
            clusterService,
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            new DatafeedManager(
                datafeedConfigProvider,
                jobConfigProvider,
                NamedXContentRegistry.EMPTY,
                settings,
                clusterService,
                client,
                mock(MachineLearningExtension.class),
                mock(AnomalyDetectionAuditor.class),
                mock(AnnotationPersister.class),
                mock(JobResultsProvider.class)
            ),
            projectResolver
        );
    }

    private static ClusterState clusterStateWithMinTransportVersion(TransportVersion transportVersion) {
        return ClusterState.builder(new ClusterName("update-datafeed-action-tests"))
            .putCompatibilityVersions("node-1", transportVersion, SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS)
            .build();
    }

    private static ClusterState coordinatingStateWithOlderMaster() {
        DiscoveryNode coordinatingNode = DiscoveryNodeUtils.create("coordinating-node");
        DiscoveryNode masterNode = DiscoveryNodeUtils.create("older-master-node");
        TransportVersion olderMasterVersion = transportVersionBeforeEsqlDatafeeds();
        return ClusterState.builder(new ClusterName("update-datafeed-action-tests"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(coordinatingNode)
                    .add(masterNode)
                    .localNodeId(coordinatingNode.getId())
                    .masterNodeId(masterNode.getId())
            )
            .putCompatibilityVersions(coordinatingNode.getId(), TransportVersion.current(), SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS)
            .putCompatibilityVersions(masterNode.getId(), olderMasterVersion, SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS)
            .build();
    }

    private static TransportVersion transportVersionBeforeEsqlDatafeeds() {
        return TransportVersionUtils.getPreviousVersion(DatafeedConfig.ML_DATAFEED_ESQL_QUERY);
    }
}
