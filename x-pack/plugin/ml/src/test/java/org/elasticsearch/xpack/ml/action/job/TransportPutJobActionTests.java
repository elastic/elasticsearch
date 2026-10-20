/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action.job;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequestHelper;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MachineLearningExtension;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportPutJobActionTests extends ESTestCase {

    private static final String JOB_ID = "embedded-job";
    private static final String SECRET = "caller-uiam-token";

    private static ThreadPool threadPool;

    private CapturingTransport transport;
    private ClusterService clusterService;
    private TransportService transportService;
    private DiscoveryNode localNode;
    private DiscoveryNode remoteNode;
    private JobManager jobManager;
    private DatafeedManager datafeedManager;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool("TransportPutJobActionTests");
    }

    @AfterClass
    public static void terminateThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Before
    public void initServices() throws Exception {
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        localNode = DiscoveryNodeUtils.builder("local_node").roles(Collections.singleton(DiscoveryNodeRole.MASTER_ROLE)).build();
        remoteNode = DiscoveryNodeUtils.builder("remote_node").roles(Collections.singleton(DiscoveryNodeRole.MASTER_ROLE)).build();
        jobManager = mock(JobManager.class);
        datafeedManager = mock(DatafeedManager.class);
    }

    @After
    public void closeServices() {
        clusterService.close();
        transportService.close();
    }

    public void testNonMasterCoordinatorWithEmbeddedDatafeedShouldForwardCallerCredentialToMaster() {
        stubCallerCredential(new CloudCredential(new SecureString(SECRET.toCharArray())));
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes()));
        PutJobAction.Request request = embeddedDatafeedRequest();

        createAction().execute(newTask(), request, new PlainActionFuture<>());

        PutJobAction.Request forwarded = forwardedToMaster();
        assertThat(forwarded.getCloudCredential(), notNullValue());
        assertThat(forwarded.getCloudCredential().value().toString(), equalTo(SECRET));
    }

    public void testNonMasterCoordinatorWithoutCallerCredentialShouldForwardRequestWithoutCredential() {
        stubCallerCredential(null);
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes()));

        createAction().execute(newTask(), embeddedDatafeedRequest(), new PlainActionFuture<>());

        assertThat(forwardedToMaster().getCloudCredential(), nullValue());
    }

    public void testNonMasterCoordinatorWithoutEmbeddedDatafeedShouldNotExtractCallerCredential() {
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes()));
        PutJobAction.Request request = new PutJobAction.Request(buildJobBuilder(JOB_ID));

        createAction().execute(newTask(), request, new PlainActionFuture<>());

        verify(datafeedManager, never()).carryCallerCredential(any(), any(), any());
        assertThat(forwardedToMaster().getCloudCredential(), nullValue());
    }

    public void testMasterOperationWithCarriedCredentialShouldPutEmbeddedDatafeedWithThatCredential() throws Exception {
        CloudCredential carried = new CloudCredential(new SecureString(SECRET.toCharArray()));
        PutJobAction.Request request = embeddedDatafeedRequest();
        request.setCloudCredential(carried);
        AtomicReference<PutDatafeedAction.Request> datafeedRequest = stubJobAndDatafeedPutSucceed();

        PlainActionFuture<PutJobAction.Response> listener = new PlainActionFuture<>();
        createAction().masterOperation(newTask(), request, ClusterState.EMPTY_STATE, listener);

        listener.actionGet();
        assertThat(datafeedRequest.get().getCloudCredential(), sameInstance(carried));
        assertThat(datafeedRequest.get().getDatafeed().getJobId(), equalTo(JOB_ID));
    }

    public void testMasterOperationWithoutCarriedCredentialShouldPutEmbeddedDatafeedWithoutCredential() throws Exception {
        AtomicReference<PutDatafeedAction.Request> datafeedRequest = stubJobAndDatafeedPutSucceed();

        PlainActionFuture<PutJobAction.Response> listener = new PlainActionFuture<>();
        createAction().masterOperation(newTask(), embeddedDatafeedRequest(), ClusterState.EMPTY_STATE, listener);

        listener.actionGet();
        assertThat(datafeedRequest.get(), notNullValue());
        assertThat(datafeedRequest.get().getCloudCredential(), nullValue());
    }

    public void testLocalMasterShouldKeepCredentialUsableDuringEmbeddedDatafeedPutAndZeroItAfterwards() {
        SecureString secret = new SecureString(SECRET.toCharArray());
        PutJobAction.Request request = embeddedDatafeedRequest();
        request.setCloudCredential(new CloudCredential(secret));
        // Master-side doExecute finds no credential locally (no transient headers): the carried one must survive untouched.
        stubCallerCredential(null);
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes()));
        AtomicReference<String> secretSeenByDatafeedPut = new AtomicReference<>();
        stubJobPutSucceeds();
        doAnswer(invocation -> {
            PutDatafeedAction.Request datafeedRequest = invocation.getArgument(0);
            secretSeenByDatafeedPut.set(datafeedRequest.getCloudCredential().value().toString());
            ActionListener<PutDatafeedAction.Response> listener = invocation.getArgument(4);
            listener.onResponse(new PutDatafeedAction.Response(datafeedRequest.getDatafeed()));
            return null;
        }).when(datafeedManager).putDatafeed(any(), any(), any(), any(), any());

        PlainActionFuture<PutJobAction.Response> listener = new PlainActionFuture<>();
        createAction().execute(newTask(), request, listener);

        listener.actionGet();
        assertThat(secretSeenByDatafeedPut.get(), equalTo(SECRET));
        expectThrows(IllegalStateException.class, secret::length);
    }

    public void testEmbeddedDatafeedPutFailureShouldDeleteJobAndSurfaceFailure() throws Exception {
        PutJobAction.Request request = embeddedDatafeedRequest();
        request.setCloudCredential(new CloudCredential(new SecureString(SECRET.toCharArray())));
        stubJobPutSucceeds();
        ElasticsearchStatusException mintFailure = new ElasticsearchStatusException(
            "Failed to grant cloud API key",
            RestStatus.UNAUTHORIZED
        );
        doAnswer(invocation -> {
            ActionListener<PutDatafeedAction.Response> listener = invocation.getArgument(4);
            listener.onFailure(mintFailure);
            return null;
        }).when(datafeedManager).putDatafeed(any(), any(), any(), any(), any());
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(jobManager).deleteJob(any(DeleteJobAction.Request.class), any(), any());

        PlainActionFuture<PutJobAction.Response> listener = new PlainActionFuture<>();
        createAction().masterOperation(newTask(), request, ClusterState.EMPTY_STATE, listener);

        Exception failure = expectThrows(Exception.class, listener::actionGet);
        assertThat(failure, sameInstance(mintFailure));
        verify(jobManager).deleteJob(any(DeleteJobAction.Request.class), any(), any());
    }

    private static final String ESQL_GATE_JOB_ID = "job-1";

    public void testEmbeddedEsqlDatafeedWhenFlagOffShouldBeRejectedBeforeCreatingJob() throws Exception {
        JobManager esqlJobManager = mock(JobManager.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        ClusterService esqlClusterService = mock(ClusterService.class);
        TransportPutJobAction action = createEsqlGatedAction(
            false,
            esqlJobManager,
            jobConfigProvider,
            datafeedConfigProvider,
            esqlClusterService
        );

        AtomicReference<Exception> failure = new AtomicReference<>();
        PutJobAction.Request request = new PutJobAction.Request(jobWithEsqlDatafeed(esqlDatafeedBuilder()));
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
        verifyNoInteractions(esqlJobManager, datafeedConfigProvider, jobConfigProvider);
    }

    public void testEmbeddedEsqlDatafeedOnMixedVersionClusterShouldBeRejectedBeforeCreatingJob() throws Exception {
        JobManager esqlJobManager = mock(JobManager.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        ClusterService esqlClusterService = mock(ClusterService.class);
        TransportPutJobAction action = createEsqlGatedAction(
            true,
            esqlJobManager,
            jobConfigProvider,
            datafeedConfigProvider,
            esqlClusterService
        );

        AtomicReference<Exception> failure = new AtomicReference<>();
        PutJobAction.Request request = new PutJobAction.Request(jobWithEsqlDatafeed(esqlDatafeedBuilder()));
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
                Messages.getMessage(
                    Messages.DATAFEED_ESQL_CREATE_UPGRADE_IN_PROGRESS,
                    "job-1",
                    "datafeed uses an ES|QL query, which requires support for ES|QL datafeeds"
                )
            )
        );
        verifyNoInteractions(esqlJobManager, datafeedConfigProvider, jobConfigProvider);
    }

    public void testCoordinatingNodeShouldRejectEmbeddedEsqlDatafeedBeforeSendingToOlderMaster() {
        JobManager esqlJobManager = mock(JobManager.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        ClusterService esqlClusterService = mock(ClusterService.class);
        when(esqlClusterService.state()).thenReturn(coordinatingStateWithOlderMaster());
        TransportPutJobAction action = createEsqlGatedAction(
            true,
            esqlJobManager,
            jobConfigProvider,
            datafeedConfigProvider,
            esqlClusterService
        );

        AtomicReference<Exception> failure = new AtomicReference<>();
        PutJobAction.Request request = new PutJobAction.Request(jobWithEsqlDatafeed(esqlDatafeedBuilder()));
        action.doExecute(null, request, ActionTestUtils.assertNoSuccessListener(failure::set));

        assertThat(failure.get(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure.get()).status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(failure.get().getMessage(), containsString("cluster upgrade is in progress"));
        verifyNoInteractions(esqlJobManager, datafeedConfigProvider, jobConfigProvider);
    }

    public void testEmbeddedEsqlDatafeedWhenFlagOnAndClusterSupportsItShouldCreateJob() throws Exception {
        JobManager esqlJobManager = mock(JobManager.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        ClusterService esqlClusterService = mock(ClusterService.class);
        TransportPutJobAction action = createEsqlGatedAction(
            true,
            esqlJobManager,
            jobConfigProvider,
            datafeedConfigProvider,
            esqlClusterService
        );

        PutJobAction.Request request = new PutJobAction.Request(jobWithEsqlDatafeed(esqlDatafeedBuilder()));
        action.masterOperation(
            null,
            request,
            clusterStateWithMinTransportVersion(TransportVersion.current()),
            ActionTestUtils.assertNoFailureListener(response -> {})
        );

        verify(esqlJobManager).putJob(any(), any(), any(), any());
    }

    public void testEmbeddedNonEsqlDatafeedWhenFlagOffOnMixedVersionClusterShouldCreateJob() throws Exception {
        JobManager esqlJobManager = mock(JobManager.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        ClusterService esqlClusterService = mock(ClusterService.class);
        TransportPutJobAction action = createEsqlGatedAction(
            false,
            esqlJobManager,
            jobConfigProvider,
            datafeedConfigProvider,
            esqlClusterService
        );
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder().setIndices(List.of("index-1"));

        PutJobAction.Request request = new PutJobAction.Request(jobWithEsqlDatafeed(datafeed));
        action.masterOperation(
            null,
            request,
            clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion()),
            ActionTestUtils.assertNoFailureListener(response -> {})
        );

        verify(esqlJobManager).putJob(any(), any(), any(), any());
    }

    public void testJobWithoutDatafeedWhenFlagOffOnMixedVersionClusterShouldCreateJob() throws Exception {
        JobManager esqlJobManager = mock(JobManager.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        ClusterService esqlClusterService = mock(ClusterService.class);
        TransportPutJobAction action = createEsqlGatedAction(
            false,
            esqlJobManager,
            jobConfigProvider,
            datafeedConfigProvider,
            esqlClusterService
        );

        PutJobAction.Request request = new PutJobAction.Request(jobWithEsqlDatafeed(null));
        action.masterOperation(
            null,
            request,
            clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion()),
            ActionTestUtils.assertNoFailureListener(response -> {})
        );

        verify(esqlJobManager).putJob(any(), any(), any(), any());
    }

    private TransportPutJobAction createEsqlGatedAction(
        boolean esqlDatafeedsEnabled,
        JobManager esqlJobManager,
        JobConfigProvider jobConfigProvider,
        DatafeedConfigProvider datafeedConfigProvider,
        ClusterService esqlClusterService
    ) {
        Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
        ClusterService datafeedManagerClusterService = mock(ClusterService.class);
        when(datafeedManagerClusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Set.of(MachineLearning.REQUIRE_ROLLBACK_SNAPSHOT_BEFORE_SCOPE_CHANGE))
        );
        DatafeedManager realDatafeedManager = new DatafeedManager(
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
            esqlClusterService,
            mock(ThreadPool.class),
            mock(XPackLicenseState.class),
            mock(ActionFilters.class),
            esqlJobManager,
            realDatafeedManager,
            null,
            projectResolver,
            () -> esqlDatafeedsEnabled
        );
    }

    private static Job.Builder jobWithEsqlDatafeed(DatafeedConfig.Builder datafeed) {
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(List.of(new Detector.Builder("count", null).build()));
        Job.Builder job = new Job.Builder(ESQL_GATE_JOB_ID).setAnalysisConfig(analysisConfig)
            .setDataDescription(new DataDescription.Builder());
        if (datafeed != null) {
            job.setDatafeed(datafeed);
        }
        return job;
    }

    private static DatafeedConfig.Builder esqlDatafeedBuilder() {
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

    private DiscoveryNode[] allNodes() {
        return new DiscoveryNode[] { localNode, remoteNode };
    }

    private TransportPutJobAction createAction() {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(MachineLearningField.ML_API_FEATURE)).thenReturn(true);
        return new TransportPutJobAction(
            Settings.EMPTY,
            transportService,
            clusterService,
            threadPool,
            licenseState,
            new ActionFilters(Collections.emptySet()),
            jobManager,
            datafeedManager,
            null, // AnalysisRegistry is final and only handed through to the mocked JobManager
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );
    }

    private static Task newTask() {
        return new Task(randomNonNegativeLong(), "test", PutJobAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static PutJobAction.Request embeddedDatafeedRequest() {
        Job.Builder job = buildJobBuilder(JOB_ID);
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder(JOB_ID, JOB_ID);
        datafeed.setIndices(List.of("logs"));
        job.setDatafeed(datafeed);
        return new PutJobAction.Request(job);
    }

    /** Simulates the coordinating node's extraction: the shared helper hands {@code credential} (if any) to the request carrier. */
    @SuppressWarnings("unchecked")
    private void stubCallerCredential(CloudCredential credential) {
        doAnswer(invocation -> {
            if (credential != null) {
                ((Consumer<CloudCredential>) invocation.getArgument(2)).accept(credential);
            }
            return null;
        }).when(datafeedManager).carryCallerCredential(any(), any(), any());
    }

    private PutJobAction.Request forwardedToMaster() {
        CapturingTransport.CapturedRequest[] captured = transport.getCapturedRequestsAndClear();
        assertThat(captured.length, equalTo(1));
        assertThat(captured[0].action(), equalTo(PutJobAction.NAME));
        assertThat(captured[0].node(), equalTo(remoteNode));
        Object unwrapped = MasterNodeRequestHelper.unwrapTermOverride(captured[0].request());
        assertThat(unwrapped, instanceOf(PutJobAction.Request.class));
        return (PutJobAction.Request) unwrapped;
    }

    private void stubJobPutSucceeds() {
        try {
            doAnswer(invocation -> {
                PutJobAction.Request request = invocation.getArgument(0);
                ActionListener<PutJobAction.Response> listener = invocation.getArgument(3);
                listener.onResponse(new PutJobAction.Response(request.getJobBuilder().build(new Date())));
                return null;
            }).when(jobManager).putJob(any(), any(), any(), any());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private AtomicReference<PutDatafeedAction.Request> stubJobAndDatafeedPutSucceed() {
        stubJobPutSucceeds();
        AtomicReference<PutDatafeedAction.Request> captured = new AtomicReference<>();
        doAnswer(invocation -> {
            PutDatafeedAction.Request datafeedRequest = invocation.getArgument(0);
            captured.set(datafeedRequest);
            ActionListener<PutDatafeedAction.Response> listener = invocation.getArgument(4);
            listener.onResponse(new PutDatafeedAction.Response(datafeedRequest.getDatafeed()));
            return null;
        }).when(datafeedManager).putDatafeed(any(), any(), any(), any(), any());
        return captured;
    }
}
