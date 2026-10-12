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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MachineLearningExtension;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportPutDatafeedActionTests extends ESTestCase {

    public void testEsqlDatafeedWhenFlagOnShouldPutDatafeed() {
        // Flag-off rejection is covered by DatafeedEsqlGatesTests; release builds need -Des.esql_datafeeds_feature_flag_enabled=true.
        assumeTrue("Only relevant when the ES|QL datafeeds feature flag is on", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        DatafeedManager datafeedManager = createDatafeedManager(datafeedConfigProvider, jobConfigProvider, client);
        ProjectResolver projectResolver = mock(ProjectResolver.class);
        when(projectResolver.getProjectId()).thenReturn(ProjectId.DEFAULT);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        TransportPutDatafeedAction action = createAction(datafeedManager, projectResolver, threadPool);
        ClusterState clusterState = clusterStateWithMinTransportVersion(TransportVersion.current());

        try (PutDatafeedAction.Request request = new PutDatafeedAction.Request(esqlDatafeed())) {
            action.masterOperation(null, request, clusterState, ActionTestUtils.assertNoFailureListener(response -> {}));

            verify(datafeedConfigProvider).findDatafeedIdsForJobIds(eq(List.of("job-1")), any());
        }
    }

    public void testMasterOperationEsqlDatafeedOnMixedVersionClusterShouldBeRejected() {
        DatafeedConfig datafeedConfig = esqlDatafeed();
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        TransportPutDatafeedAction action = createAction(datafeedConfigProvider, jobConfigProvider, client);
        ClusterState clusterState = clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion());

        try (PutDatafeedAction.Request request = new PutDatafeedAction.Request(datafeedConfig)) {
            AtomicReference<Exception> failure = new AtomicReference<>();
            action.masterOperation(null, request, clusterState, ActionTestUtils.assertNoSuccessListener(failure::set));

            assertThat(failure.get(), instanceOf(ElasticsearchStatusException.class));
            assertThat(((ElasticsearchStatusException) failure.get()).status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(
                failure.get().getMessage(),
                equalTo(
                    Messages.getMessage(
                        Messages.DATAFEED_ESQL_CREATE_UPGRADE_IN_PROGRESS,
                        "datafeed-1",
                        "datafeed uses an ES|QL query, which requires support for ES|QL datafeeds"
                    )
                )
            );
            verifyNoInteractions(datafeedConfigProvider, jobConfigProvider, client);
        }
    }

    public void testCoordinatingNodeShouldRejectEsqlDatafeedBeforeSendingToOlderMaster() {
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        TransportService transportService = mock(TransportService.class);
        TransportPutDatafeedAction action = createAction(
            datafeedConfigProvider,
            jobConfigProvider,
            client,
            clusterService,
            transportService
        );
        when(clusterService.state()).thenReturn(coordinatingStateWithOlderMaster());
        clearInvocations(transportService);

        AtomicReference<Exception> failure = new AtomicReference<>();
        action.doExecute(null, new PutDatafeedAction.Request(esqlDatafeed()), ActionTestUtils.assertNoSuccessListener(failure::set));

        assertThat(failure.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure.get()).status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(failure.get().getMessage(), containsString("cluster upgrade is in progress"));
        assertThat(failure.get().getMessage(), containsString("wait for the cluster to finish upgrading"));
        verifyNoInteractions(transportService, datafeedConfigProvider, jobConfigProvider, client);
    }

    private static DatafeedConfig esqlDatafeed() {
        return new DatafeedConfig.Builder("datafeed-1", "job-1").setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .build();
    }

    private static TransportPutDatafeedAction createAction(
        DatafeedConfigProvider datafeedConfigProvider,
        JobConfigProvider jobConfigProvider,
        Client client
    ) {
        return createAction(datafeedConfigProvider, jobConfigProvider, client, mock(ClusterService.class), mock(TransportService.class));
    }

    private static TransportPutDatafeedAction createAction(
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
        DatafeedManager datafeedManager = createDatafeedManager(datafeedConfigProvider, jobConfigProvider, client);
        ProjectResolver projectResolver = mock(ProjectResolver.class);
        when(projectResolver.getProjectId()).thenReturn(ProjectId.DEFAULT);
        return new TransportPutDatafeedAction(
            settings,
            transportService,
            clusterService,
            mock(ThreadPool.class),
            mock(XPackLicenseState.class),
            mock(ActionFilters.class),
            datafeedManager,
            projectResolver
        );
    }

    private static DatafeedManager createDatafeedManager(
        DatafeedConfigProvider datafeedConfigProvider,
        JobConfigProvider jobConfigProvider,
        Client client
    ) {
        Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Set.of(MachineLearning.REQUIRE_ROLLBACK_SNAPSHOT_BEFORE_SCOPE_CHANGE))
        );
        return new DatafeedManager(
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
        );
    }

    private static TransportPutDatafeedAction createAction(
        DatafeedManager datafeedManager,
        ProjectResolver projectResolver,
        ThreadPool threadPool
    ) {
        Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Set.of(MachineLearning.REQUIRE_ROLLBACK_SNAPSHOT_BEFORE_SCOPE_CHANGE))
        );
        return new TransportPutDatafeedAction(
            settings,
            mock(TransportService.class),
            clusterService,
            threadPool,
            mock(XPackLicenseState.class),
            mock(ActionFilters.class),
            datafeedManager,
            projectResolver
        );
    }

    private static ClusterState clusterStateWithMinTransportVersion(TransportVersion transportVersion) {
        return ClusterState.builder(new ClusterName("put-datafeed-action-tests"))
            .putCompatibilityVersions("node-1", transportVersion, SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS)
            .build();
    }

    private static ClusterState coordinatingStateWithOlderMaster() {
        DiscoveryNode coordinatingNode = DiscoveryNodeUtils.create("coordinating-node");
        DiscoveryNode masterNode = DiscoveryNodeUtils.create("older-master-node");
        return ClusterState.builder(new ClusterName("put-datafeed-action-tests"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(coordinatingNode)
                    .add(masterNode)
                    .localNodeId(coordinatingNode.getId())
                    .masterNodeId(masterNode.getId())
            )
            .putCompatibilityVersions(coordinatingNode.getId(), TransportVersion.current(), SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS)
            .putCompatibilityVersions(masterNode.getId(), preEsqlDatafeedTransportVersion(), SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS)
            .build();
    }

    private static TransportVersion preEsqlDatafeedTransportVersion() {
        return TransportVersion.fromName("histogram_blocks_multivalue_support");
    }
}
