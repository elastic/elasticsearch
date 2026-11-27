/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action.datafeed;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialsExtension;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.DatafeedRunner;
import org.elasticsearch.xpack.ml.datafeed.DatafeedRunnerTests;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDatafeedQueryValidator;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT;
import static org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests.addJobTask;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportStartDatafeedActionTests extends ESTestCase {

    public void testEsqlDatafeedWhenFlagOnShouldAllowStart() {
        // ESQL_DATAFEEDS_FEATURE_FLAG is on in snapshot/test JVMs and fixed for the process lifetime, so the
        // flag-off rejection path (Messages.DATAFEED_ESQL_START_DISABLED) is covered in DatafeedEsqlGatesTests;
        // on a release build, verify with -Des.esql_datafeeds_feature_flag_enabled=false.
        assumeTrue("Only relevant when the ES|QL datafeeds feature flag is on", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        DatafeedConfig datafeed = new DatafeedConfig.Builder("esql-datafeed", "job").setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .build();
        // Does not throw: the feature flag is on and the cluster is fully upgraded.
        TransportStartDatafeedAction.validateEsqlDatafeedEnabled(datafeed, currentCompatibleClusterState());
    }

    public void testStoredEsqlDatafeedOnMixedVersionClusterShouldRejectStart() {
        DatafeedConfig datafeed = new DatafeedConfig.Builder("esql-datafeed", "job").setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .putCompatibilityVersions(
                "older-node",
                TransportVersion.fromName("histogram_blocks_multivalue_support"),
                SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS
            )
            .build();

        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportStartDatafeedAction.validateEsqlDatafeedEnabled(datafeed, state)
        );
        assertThat(exception.getMessage(), containsString("cluster upgrade is in progress"));
        assertThat(exception.getMessage(), containsString("before restoring or starting it"));
    }

    public void testClassicDatafeedAlwaysAllowedToStart() {
        DatafeedConfig datafeed = new DatafeedConfig.Builder("classic-datafeed", "job").setIndices(List.of("logs")).build();
        TransportStartDatafeedAction.validateEsqlDatafeedEnabled(datafeed, ClusterState.builder(new ClusterName("test")).build());
    }

    private static ClusterState currentCompatibleClusterState() {
        return ClusterState.builder(new ClusterName("test"))
            .putCompatibilityVersions("current-node", TransportVersion.current(), SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS)
            .build();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testValidate_jobClosed() {
        Job job1 = DatafeedRunnerTests.createDatafeedJob().build(new Date());
        PersistentTasksCustomMetadata tasks = PersistentTasksCustomMetadata.builder().build();
        DatafeedConfig datafeedConfig1 = DatafeedRunnerTests.createDatafeedConfig("foo-datafeed", "job_id").build();
        Exception e = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportStartDatafeedAction.validate(job1, datafeedConfig1, tasks, xContentRegistry())
        );
        assertThat(e.getMessage(), equalTo("cannot start datafeed [foo-datafeed] because job [job_id] is closed"));
    }

    public void testValidate_jobOpening() {
        Job job1 = DatafeedRunnerTests.createDatafeedJob().build(new Date());
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job_id", INITIAL_ASSIGNMENT.getExecutorNode(), null, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();
        DatafeedConfig datafeedConfig1 = DatafeedRunnerTests.createDatafeedConfig("foo-datafeed", "job_id").build();

        TransportStartDatafeedAction.validate(job1, datafeedConfig1, tasks, xContentRegistry());
    }

    public void testValidate_jobOpened() {
        Job job1 = DatafeedRunnerTests.createDatafeedJob().build(new Date());
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job_id", INITIAL_ASSIGNMENT.getExecutorNode(), JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();
        DatafeedConfig datafeedConfig1 = DatafeedRunnerTests.createDatafeedConfig("foo-datafeed", "job_id").build();

        TransportStartDatafeedAction.validate(job1, datafeedConfig1, tasks, xContentRegistry());
    }

    public void testDeprecationsLogged() {
        Job job1 = DatafeedRunnerTests.createDatafeedJob().build(new Date());
        DatafeedConfig.Builder datafeedConfig = DatafeedRunnerTests.createDatafeedConfig("start-data-feed-test", job1.getId());
        DatafeedConfig config = spy(datafeedConfig.build());
        doReturn(Collections.singletonList("Deprecated Agg")).when(config).getAggDeprecations(any(NamedXContentRegistry.class));
        doReturn(Collections.singletonList("Deprecated Query")).when(config).getQueryDeprecations(any(NamedXContentRegistry.class));

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);

        TransportStartDatafeedAction.auditDeprecations(config, job1, auditor, xContentRegistry());

        verify(auditor).warning(
            job1.getId(),
            "datafeed [start-data-feed-test] configuration has deprecations. [Deprecated Agg, Deprecated Query]"
        );
    }

    public void testNoDeprecationsLogged() {
        Job job1 = DatafeedRunnerTests.createDatafeedJob().build(new Date());
        DatafeedConfig.Builder datafeedConfig = DatafeedRunnerTests.createDatafeedConfig("start-data-feed-test", job1.getId());
        DatafeedConfig config = spy(datafeedConfig.build());
        doReturn(Collections.emptyList()).when(config).getAggDeprecations(any(NamedXContentRegistry.class));
        doReturn(Collections.emptyList()).when(config).getQueryDeprecations(any(NamedXContentRegistry.class));

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);

        TransportStartDatafeedAction.auditDeprecations(config, job1, auditor, xContentRegistry());

        verify(auditor, never()).warning(any(), any());
    }

    public void testStartDatafeedOriginQualifiedIndexExcludedFromRemoteClusterChecks() {
        assertThat(TransportStartDatafeedAction.trueRemoteIndices(List.of("_origin:foo")), equalTo(List.of()));
        assertThat(TransportStartDatafeedAction.trueRemoteIndices(List.of("_origin:*")), equalTo(List.of()));
        assertThat(
            TransportStartDatafeedAction.trueRemoteIndices(List.of("remote:foo", "_origin:bar", "local")),
            equalTo(List.of("remote:foo"))
        );
        assertThat(
            RemoteClusterLicenseChecker.remoteClusterAliases(
                Set.of(),
                TransportStartDatafeedAction.trueRemoteIndices(List.of("_origin:foo"))
            ),
            equalTo(List.of())
        );
        expectThrows(
            NoSuchRemoteClusterException.class,
            () -> RemoteClusterLicenseChecker.remoteClusterAliases(Set.of(), List.of("_origin:foo"))
        );
    }

    public void testResolveRemoteSourcesGivenEsqlDatafeedShouldProbeWithHeadersAndEffectiveProjectRouting() {
        EsqlDatafeedQueryValidator validator = mock(EsqlDatafeedQueryValidator.class);
        Client probeClient = mock(Client.class);
        DatafeedConfig datafeed = newEsqlDatafeed().setHeaders(Map.of("h", "v")).setProjectRouting("_alias:prod-*").build();
        List<String> remote = List.of("east:logs-*");
        doAnswer(invocation -> {
            invocation.<ActionListener<List<String>>>getArgument(6).onResponse(remote);
            return null;
        }).when(validator).validateAtStart(any(), any(), any(), any(), any(), any(), any(), any());

        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams("esql-datafeed", 0L);
        PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        TransportStartDatafeedAction.resolveRemoteSources(
            validator,
            () -> probeClient,
            datafeed,
            newJob(),
            new CrossProjectModeDecider(Settings.EMPTY),
            params,
            future
        );

        assertThat(future.actionGet(), equalTo(remote));
        assertThat(params.getDatafeedIndices(), equalTo(remote));
        // cross-project search is off, so the persisted project routing is inert and must not reach ES|QL
        verify(validator).validateAtStart(
            same(probeClient),
            eq(Map.of("h", "v")),
            eq(datafeed.getEsqlQuery()),
            isNull(),
            eq("time"),
            isNull(),
            any(),
            eq("esql-datafeed")
        );
    }

    public void testResolveRemoteSourcesGivenProbeFailureShouldFailListener() {
        EsqlDatafeedQueryValidator validator = mock(EsqlDatafeedQueryValidator.class);
        DatafeedConfig datafeed = newEsqlDatafeed().build();
        doAnswer(invocation -> {
            invocation.<ActionListener<List<String>>>getArgument(6).onFailure(new IllegalStateException("boom"));
            return null;
        }).when(validator).validateAtStart(any(), any(), any(), any(), any(), any(), any(), any());

        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams("esql-datafeed", 0L);
        PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        TransportStartDatafeedAction.resolveRemoteSources(
            validator,
            () -> mock(Client.class),
            datafeed,
            newJob(),
            new CrossProjectModeDecider(Settings.EMPTY),
            params,
            future
        );

        expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(params.getDatafeedIndices(), equalTo(List.of()));
    }

    public void testResolveRemoteSourcesGivenEsqlDatafeedWithCrossProjectEnabledShouldNotProbe() {
        assumeTrue("CPS feature flag must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());
        EsqlDatafeedQueryValidator validator = mock(EsqlDatafeedQueryValidator.class);
        DatafeedConfig datafeed = newEsqlDatafeed().build();
        CrossProjectModeDecider decider = new CrossProjectModeDecider(
            Settings.builder().put("serverless.cross_project.enabled", true).build()
        );

        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams("esql-datafeed", 0L);
        PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        TransportStartDatafeedAction.resolveRemoteSources(validator, () -> mock(Client.class), datafeed, newJob(), decider, params, future);

        assertThat(future.actionGet(), equalTo(List.of()));
        verifyNoInteractions(validator);
        assertThat(TransportStartDatafeedAction.skipsRemoteClusterChecks(datafeed, decider), is(true));
    }

    public void testResolveRemoteSourcesGivenDslDatafeedShouldStoreIndicesAndReturnRemoteOnesWithoutProbing() {
        EsqlDatafeedQueryValidator validator = mock(EsqlDatafeedQueryValidator.class);
        DatafeedConfig datafeed = new DatafeedConfig.Builder("dsl-datafeed", "job_id").setIndices(
            List.of("local-index", "east:logs-*", "_origin:logs-*")
        ).build();

        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams("dsl-datafeed", 0L);
        PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        TransportStartDatafeedAction.resolveRemoteSources(
            validator,
            () -> { throw new AssertionError("a DSL datafeed is not probed"); },
            datafeed,
            newJob(),
            new CrossProjectModeDecider(Settings.EMPTY),
            params,
            future
        );

        assertThat(future.actionGet(), equalTo(List.of("east:logs-*")));
        assertThat(params.getDatafeedIndices(), equalTo(datafeed.getIndices()));
        verifyNoInteractions(validator);
    }

    public void testMasterOperationGivenEsqlProbeFailureOnNodeWithoutRemoteClusterClientShouldPropagateItWithoutCreatingTask() {
        assumeTrue("Only relevant when the ES|QL datafeeds feature flag is on", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        EsqlDatafeedQueryValidator validator = mock(EsqlDatafeedQueryValidator.class);
        // ES|QL rejects a query with a cluster prefix on a node without remote_cluster_client, so the probe fails before it can list
        // sources.
        ElasticsearchStatusException probeFailure = new ElasticsearchStatusException(
            "node [master-1] does not have the remote_cluster_client role",
            RestStatus.BAD_REQUEST
        );
        doAnswer(invocation -> {
            invocation.<ActionListener<List<String>>>getArgument(6).onFailure(probeFailure);
            return null;
        }).when(validator).validateAtStart(any(), any(), any(), any(), any(), any(), any(), any());
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        doAnswer(invocation -> {
            invocation.<ActionListener<DatafeedConfig.Builder>>getArgument(2)
                .onResponse(newEsqlDatafeed().setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig()));
            return null;
        }).when(datafeedConfigProvider).getDatafeedConfig(eq("esql-datafeed"), isNull(), any());
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        doAnswer(invocation -> {
            invocation.<ActionListener<Job.Builder>>getArgument(2).onResponse(new Job.Builder(newJob()));
            return null;
        }).when(jobConfigProvider).getJob(eq("job_id"), isNull(), any());
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create("master-1"));
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(MachineLearningField.ML_API_FEATURE)).thenReturn(true);
        PersistentTasksService persistentTasksService = mock(PersistentTasksService.class);
        TransportStartDatafeedAction action = new TransportStartDatafeedAction(
            Settings.builder().putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "master", "ml").build(),
            mock(TransportService.class),
            threadPool,
            clusterService,
            licenseState,
            persistentTasksService,
            mock(ActionFilters.class),
            mock(Client.class),
            jobConfigProvider,
            datafeedConfigProvider,
            mock(AnomalyDetectionAuditor.class),
            xContentRegistry(),
            mock(ProjectResolver.class),
            validator
        );
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        addJobTask("job_id", "node_id", JobState.OPENED, tasks);
        ClusterState state = ClusterState.builder(currentCompatibleClusterState())
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build()))
            .build();

        AtomicReference<Exception> failure = new AtomicReference<>();
        action.masterOperation(
            new Task(1L, "transport", StartDatafeedAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of()),
            new StartDatafeedAction.Request("esql-datafeed", 0L),
            state,
            ActionTestUtils.assertNoSuccessListener(failure::set)
        );

        assertThat(failure.get(), sameInstance(probeFailure));
        verifyNoInteractions(persistentTasksService);
    }

    public void testDatafeedTaskShouldExposeDatafeedIndicesFromParams() {
        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams("esql-datafeed", 0L);
        params.setDatafeedIndices(List.of("east:logs-*"));

        TransportStartDatafeedAction.DatafeedTask task = createDatafeedTask(
            1L,
            "type",
            "action",
            new TaskId("node", 1L),
            params,
            mock(DatafeedRunner.class)
        );

        assertThat(task.getDatafeedIndices(), equalTo(List.of("east:logs-*")));
    }

    private static DatafeedConfig.Builder newEsqlDatafeed() {
        return new DatafeedConfig.Builder("esql-datafeed", "job_id").setEsqlQuery("FROM east:logs-*")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1));
    }

    private static Job newJob() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder job = DatafeedRunnerTests.createDatafeedJob();
        job.setDataDescription(dataDescription);
        job.setCreateTime(new Date());
        return job.build();
    }

    public void testStartEsqlDatafeedWithCrossProjectEnabledSkipsIndicesOptions() {
        assumeTrue("CPS feature flag must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());

        DatafeedConfig esqlDatafeed = new DatafeedConfig.Builder("esql-datafeed", "job_id").setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .build();

        CrossProjectModeDecider decider = new CrossProjectModeDecider(
            Settings.builder().put("serverless.cross_project.enabled", true).build()
        );

        StartDatafeedAction.DatafeedParams params = spy(new StartDatafeedAction.DatafeedParams("esql-datafeed", 0L));
        DatafeedConfig effectiveDatafeed = DatafeedConfig.withCrossProjectModeIfEnabled(
            esqlDatafeed,
            decider,
            esqlDatafeed.getCloudInternalCredential() != null
        );
        TransportStartDatafeedAction.setIndicesOptionsIfPresent(params, effectiveDatafeed);

        assertThat(effectiveDatafeed, sameInstance(esqlDatafeed));
        assertThat(effectiveDatafeed.getIndicesOptions(), nullValue());
        assertThat(TransportStartDatafeedAction.isCrossProjectMode(effectiveDatafeed), is(false));
        verify(params, never()).setIndicesOptions(any());
        assertThat(params.getIndicesOptions(), sameInstance(SearchRequest.DEFAULT_INDICES_OPTIONS));
    }

    public void testStartSkipsFactoryValidationOnlyForEsqlDatafeeds() {
        // The start action must not build an ES|QL extractor factory (it would run a second | DESCRIBE); the task node does.
        DatafeedConfig esqlDatafeed = new DatafeedConfig.Builder("esql-datafeed", "job_id").setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .build();
        DatafeedConfig classicDatafeed = new DatafeedConfig.Builder("classic-datafeed", "job_id").setIndices(List.of("logs")).build();

        CrossProjectModeDecider notCrossProject = new CrossProjectModeDecider(Settings.EMPTY);

        assertThat(TransportStartDatafeedAction.skipsFactoryValidation(esqlDatafeed, notCrossProject), is(true));
        assertThat(TransportStartDatafeedAction.skipsFactoryValidation(classicDatafeed, notCrossProject), is(false));
    }

    public void testStartValidatesEsqlDatafeedFactoryInCrossProjectMode() {
        // The master skips its | DESCRIBE in cross-project mode, so the factory creation must validate or errors surface as a timeout.
        assumeTrue("cross-project ML is off in this build", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());
        DatafeedConfig esqlDatafeed = new DatafeedConfig.Builder("esql-datafeed", "job_id").setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .build();
        CrossProjectModeDecider crossProject = new CrossProjectModeDecider(
            Settings.builder().put("serverless.cross_project.enabled", true).build()
        );

        assertThat(TransportStartDatafeedAction.skipsFactoryValidation(esqlDatafeed, crossProject), is(false));
    }

    public void testStartClassicDatafeedSetsIndicesOptions() {
        DatafeedConfig classicDatafeed = new DatafeedConfig.Builder("classic-datafeed", "job_id").setIndices(List.of("logs"))
            .setIndicesOptions(org.elasticsearch.action.support.IndicesOptions.STRICT_EXPAND_OPEN)
            .build();
        StartDatafeedAction.DatafeedParams params = spy(new StartDatafeedAction.DatafeedParams("classic-datafeed", 0L));

        TransportStartDatafeedAction.setIndicesOptionsIfPresent(params, classicDatafeed);

        verify(params).setIndicesOptions(org.elasticsearch.action.support.IndicesOptions.STRICT_EXPAND_OPEN);
        assertThat(params.getIndicesOptions(), sameInstance(org.elasticsearch.action.support.IndicesOptions.STRICT_EXPAND_OPEN));
    }

    public static TransportStartDatafeedAction.DatafeedTask createDatafeedTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        StartDatafeedAction.DatafeedParams params,
        DatafeedRunner datafeedRunner
    ) {
        TransportStartDatafeedAction.DatafeedTask task = new TransportStartDatafeedAction.DatafeedTask(
            id,
            type,
            action,
            parentTaskId,
            params,
            Collections.emptyMap()
        );
        assertThat(task.setDatafeedRunner(datafeedRunner), is(TransportStartDatafeedAction.DatafeedTask.StoppedOrIsolated.NEITHER));
        return task;
    }
}
