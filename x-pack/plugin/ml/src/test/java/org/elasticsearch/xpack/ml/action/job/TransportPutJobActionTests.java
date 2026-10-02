/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action.job;

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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MachineLearningExtension;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportPutJobActionTests extends ESTestCase {

    private static final String JOB_ID = "job-1";

    private JobManager jobManager;
    private DatafeedConfigProvider datafeedConfigProvider;
    private JobConfigProvider jobConfigProvider;
    private ClusterService clusterService;

    public void testEmbeddedEsqlDatafeedWhenFlagOffShouldBeRejectedBeforeCreatingJob() throws Exception {
        TransportPutJobAction action = createAction(false);

        AtomicReference<Exception> failure = new AtomicReference<>();
        PutJobAction.Request request = new PutJobAction.Request(jobWithDatafeed(esqlDatafeed()));
        action.masterOperation(
            null,
            request,
            clusterStateWithMinTransportVersion(TransportVersion.current()),
            ActionTestUtils.assertNoSuccessListener(failure::set)
        );

        assertThat(failure.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure.get()).status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            failure.get().getMessage(),
            equalTo("Cannot create ES|QL datafeed [job-1] because ES|QL datafeeds are not enabled on this node.")
        );
        verifyNoInteractions(jobManager, datafeedConfigProvider, jobConfigProvider);
    }

    public void testEmbeddedEsqlDatafeedOnMixedVersionClusterShouldBeRejectedBeforeCreatingJob() throws Exception {
        TransportPutJobAction action = createAction(true);

        AtomicReference<Exception> failure = new AtomicReference<>();
        PutJobAction.Request request = new PutJobAction.Request(jobWithDatafeed(esqlDatafeed()));
        action.masterOperation(
            null,
            request,
            clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion()),
            ActionTestUtils.assertNoSuccessListener(failure::set)
        );

        assertThat(failure.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure.get()).status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            failure.get().getMessage(),
            equalTo(
                "Cannot create datafeed [job-1] while a cluster upgrade is in progress "
                    + "(datafeed uses an ES|QL query, which requires support for ES|QL datafeeds); "
                    + "wait for the cluster to finish upgrading and try again."
            )
        );
        verifyNoInteractions(jobManager, datafeedConfigProvider, jobConfigProvider);
    }

    public void testCoordinatingNodeShouldRejectEmbeddedEsqlDatafeedBeforeSendingToOlderMaster() {
        TransportPutJobAction action = createAction(true);
        when(clusterService.state()).thenReturn(coordinatingStateWithOlderMaster());

        AtomicReference<Exception> failure = new AtomicReference<>();
        PutJobAction.Request request = new PutJobAction.Request(jobWithDatafeed(esqlDatafeed()));
        action.doExecute(null, request, ActionTestUtils.assertNoSuccessListener(failure::set));

        assertThat(failure.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure.get()).status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(failure.get().getMessage(), containsString("cluster upgrade is in progress"));
        verifyNoInteractions(jobManager, datafeedConfigProvider, jobConfigProvider);
    }

    public void testEmbeddedEsqlDatafeedWhenFlagOnAndClusterSupportsItShouldCreateJob() throws Exception {
        TransportPutJobAction action = createAction(true);

        PutJobAction.Request request = new PutJobAction.Request(jobWithDatafeed(esqlDatafeed()));
        action.masterOperation(
            null,
            request,
            clusterStateWithMinTransportVersion(TransportVersion.current()),
            ActionTestUtils.assertNoFailureListener(response -> {})
        );

        verify(jobManager).putJob(any(), any(), any(), any());
    }

    public void testEmbeddedNonEsqlDatafeedWhenFlagOffOnMixedVersionClusterShouldCreateJob() throws Exception {
        TransportPutJobAction action = createAction(false);
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder().setIndices(List.of("index-1"));

        PutJobAction.Request request = new PutJobAction.Request(jobWithDatafeed(datafeed));
        action.masterOperation(
            null,
            request,
            clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion()),
            ActionTestUtils.assertNoFailureListener(response -> {})
        );

        verify(jobManager).putJob(any(), any(), any(), any());
    }

    public void testJobWithoutDatafeedWhenFlagOffOnMixedVersionClusterShouldCreateJob() throws Exception {
        TransportPutJobAction action = createAction(false);

        PutJobAction.Request request = new PutJobAction.Request(jobWithDatafeed(null));
        action.masterOperation(
            null,
            request,
            clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion()),
            ActionTestUtils.assertNoFailureListener(response -> {})
        );

        verify(jobManager).putJob(any(), any(), any(), any());
    }

    private TransportPutJobAction createAction(boolean esqlDatafeedsEnabled) {
        jobManager = mock(JobManager.class);
        datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        jobConfigProvider = mock(JobConfigProvider.class);
        clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
        ClusterService datafeedManagerClusterService = mock(ClusterService.class);
        when(datafeedManagerClusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Set.of(MachineLearning.REQUIRE_ROLLBACK_SNAPSHOT_BEFORE_SCOPE_CHANGE))
        );
        // DatafeedManager is final, so build a real one over mocked providers
        DatafeedManager datafeedManager = new DatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            NamedXContentRegistry.EMPTY,
            settings,
            datafeedManagerClusterService,
            mock(Client.class),
            mock(MachineLearningExtension.class),
            mock(AnomalyDetectionAuditor.class),
            mock(AnnotationPersister.class),
            mock(JobResultsProvider.class)
        );
        ProjectResolver projectResolver = mock(ProjectResolver.class);
        when(projectResolver.getProjectId()).thenReturn(ProjectId.DEFAULT);
        return new TransportPutJobAction(
            settings,
            mock(TransportService.class),
            clusterService,
            mock(ThreadPool.class),
            mock(XPackLicenseState.class),
            mock(ActionFilters.class),
            jobManager,
            datafeedManager,
            null,
            projectResolver,
            () -> esqlDatafeedsEnabled
        );
    }

    private static Job.Builder jobWithDatafeed(DatafeedConfig.Builder datafeed) {
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(List.of(new Detector.Builder("count", null).build()));
        Job.Builder job = new Job.Builder(JOB_ID).setAnalysisConfig(analysisConfig).setDataDescription(new DataDescription.Builder());
        if (datafeed != null) {
            job.setDatafeed(datafeed);
        }
        return job;
    }

    private static DatafeedConfig.Builder esqlDatafeed() {
        return new DatafeedConfig.Builder().setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1));
    }

    private static ClusterState clusterStateWithMinTransportVersion(TransportVersion transportVersion) {
        return ClusterState.builder(new ClusterName("put-job-action-tests"))
            .putCompatibilityVersions("node-1", transportVersion, SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS)
            .build();
    }

    private static ClusterState coordinatingStateWithOlderMaster() {
        DiscoveryNode coordinatingNode = DiscoveryNodeUtils.create("coordinating-node");
        DiscoveryNode masterNode = DiscoveryNodeUtils.create("older-master-node");
        return ClusterState.builder(new ClusterName("put-job-action-tests"))
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
