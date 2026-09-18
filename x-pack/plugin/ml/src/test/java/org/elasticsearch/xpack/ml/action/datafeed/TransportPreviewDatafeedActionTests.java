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
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesBuilder;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.ChunkingConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.SearchIntervalTests;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialManager;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialsExtension;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MachineLearningExtensionHolder;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.chunked.ChunkedDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.security.cloud.CloudCredentialTestUtils.randomCloudCredentialEncryptedData;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPreviewDatafeedActionTests extends ESTestCase {

    public void testEsqlDatafeedWhenFlagOffShouldRejectPreview() {
        DatafeedConfig datafeed = esqlDatafeedBuilder("esql-datafeed", "job").build();
        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportPreviewDatafeedAction.validateEsqlDatafeedEnabled(datafeed, currentCompatibleClusterState(), ProjectId.DEFAULT)
        );
        assertThat(exception.getMessage(), containsString("xpack.ml.esql_datafeeds.enabled"));
        assertThat(exception.getMessage(), containsString("enable"));
        assertThat(exception.getMessage(), containsString("esql-datafeed"));
        assertThat(exception.getMessage(), not(containsString("ml_datafeed_esql_query")));
    }

    public void testStoredEsqlDatafeedOnMixedVersionClusterShouldRejectPreview() {
        DatafeedConfig datafeed = esqlDatafeedBuilder("esql-datafeed", "job").setChunkingConfig(ChunkingConfig.newOff()).build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .putCompatibilityVersions(
                "older-node",
                TransportVersion.fromName("histogram_blocks_multivalue_support"),
                SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS
            )
            .build();

        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportPreviewDatafeedAction.validateEsqlDatafeedEnabled(datafeed, state, ProjectId.DEFAULT)
        );
        assertThat(exception.getMessage(), containsString("cluster upgrade is in progress"));
        assertThat(exception.getMessage(), containsString("before restoring or starting it"));
        assertThat(exception.getMessage(), containsString("esql-datafeed"));
    }

    public void testClassicDatafeedWhenFlagOffShouldAllowPreview() {
        DatafeedConfig datafeed = new DatafeedConfig.Builder("classic-datafeed", "job").setIndices(List.of("logs")).build();
        TransportPreviewDatafeedAction.validateEsqlDatafeedEnabled(
            datafeed,
            ClusterState.builder(new ClusterName("test")).build(),
            ProjectId.DEFAULT
        );
    }

    public void testPreviewDatafeedUsesFactorySeamForEsqlDatafeed() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        MachineLearningExtensionHolder extensions = mock(MachineLearningExtensionHolder.class);
        when(extensions.isEmpty()).thenReturn(true);
        RecordingPreviewAction action = new RecordingPreviewAction(clusterService, extensions);
        DatafeedConfig datafeed = esqlDatafeedBuilder("esql-datafeed", "job").build();
        ActionListener<PreviewDatafeedAction.Response> listener = ActionListener.wrap(
            response -> {},
            e -> { throw new AssertionError(e); }
        );

        action.previewDatafeed(new TaskId("node", 1L), datafeed, mock(Job.class), mock(PreviewDatafeedAction.Request.class), listener);

        assertThat(action.factoryDatafeed, equalTo(datafeed));
        assertThat(action.factory, instanceOf(ChunkedDataExtractorFactory.class));
        assertThat(((ChunkedDataExtractorFactory) action.factory).getDelegate(), instanceOf(EsqlDataExtractorFactory.class));
    }

    public void testPreviewShouldUseRequestWindowNotPersistedCheckpoint() {
        DatafeedConfig datafeed = esqlDatafeedBuilder("esql-datafeed", "job").build();
        PreviewDatafeedAction.Request request = new PreviewDatafeedAction.Request(datafeed, null, 1_000L, 9_000_000L);
        assertThat(request.getStartTime(), equalTo(OptionalLong.of(1_000L)));
        assertThat(TransportPreviewDatafeedAction.resolvePreviewEndTime(request, true), equalTo(9_000_000L));
    }

    private static DatafeedConfig.Builder esqlDatafeedBuilder(String datafeedId, String jobId) {
        return new DatafeedConfig.Builder(datafeedId, jobId).setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1));
    }

    private class RecordingPreviewAction extends TransportPreviewDatafeedAction {
        protected DatafeedConfig factoryDatafeed;
        protected DataExtractorFactory factory;

        RecordingPreviewAction(ClusterService clusterService, MachineLearningExtensionHolder extensions) {
            super(
                Settings.EMPTY,
                threadPool,
                mock(TransportService.class),
                mock(ActionFilters.class),
                mock(Client.class),
                clusterService,
                mock(JobConfigProvider.class),
                mock(DatafeedConfigProvider.class),
                NamedXContentRegistry.EMPTY,
                extensions,
                mock(ProjectResolver.class)
            );
        }

        @Override
        void createDataExtractorFactory(
            Client client,
            CloudCredentialManager cloudCredentialManager,
            DatafeedConfig datafeed,
            QueryBuilder extraFilters,
            Job job,
            NamedXContentRegistry xContentRegistry,
            DatafeedTimingStatsReporter timingStatsReporter,
            ActionListener<DataExtractorFactory> listener
        ) {
            factoryDatafeed = datafeed;
            super.createDataExtractorFactory(
                client,
                cloudCredentialManager,
                datafeed,
                extraFilters,
                job,
                xContentRegistry,
                timingStatsReporter,
                ActionListener.wrap(createdFactory -> factory = createdFactory, listener::onFailure)
            );
        }
    }

    private static ClusterState currentCompatibleClusterState() {
        return ClusterState.builder(new ClusterName("test"))
            .putCompatibilityVersions("current-node", TransportVersion.current(), SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS)
            .build();
    }

    private DataExtractor dataExtractor;
    private ActionListener<PreviewDatafeedAction.Response> actionListener;
    private String capturedResponse;
    private Exception capturedFailure;
    private TestThreadPool threadPool;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpTests() {
        threadPool = new TestThreadPool(
            getTestName(),
            new ScalingExecutorBuilder(MachineLearning.UTILITY_THREAD_POOL_NAME, 0, 1, TimeValue.timeValueMinutes(10), false)
        );
        dataExtractor = mock(DataExtractor.class);
        actionListener = mock(ActionListener.class);

        doAnswer((Answer<Void>) invocationOnMock -> {
            PreviewDatafeedAction.Response response = (PreviewDatafeedAction.Response) invocationOnMock.getArguments()[0];
            capturedResponse = response.toString();
            return null;
        }).when(actionListener).onResponse(any());

        doAnswer((Answer<Void>) invocationOnMock -> {
            capturedFailure = (Exception) invocationOnMock.getArguments()[0];
            return null;
        }).when(actionListener).onFailure(any());
    }

    @After
    public void tearDownTests() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testBuildPreviewDatafeed_GivenPersistedCredential_ShouldClearForPreview() {
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("cred_feed", "job_foo");
        datafeed.setIndices(Collections.singletonList("my_index"));
        datafeed.setCloudInternalCredential(new PersistedCloudCredential("id", randomCloudCredentialEncryptedData()));

        DatafeedConfig previewDatafeed = TransportPreviewDatafeedAction.buildPreviewDatafeed(datafeed.build())
            .setCloudInternalCredential(null)
            .build();

        assertThat(previewDatafeed.getCloudInternalCredential(), nullValue());
    }

    public void testIsCrossProjectPreviewAllowed_GivenPersistedLegacyWithoutEnvelope_ShouldReturnFalse() {
        DatafeedConfig persisted = new DatafeedConfig.Builder("legacy_feed", "job_foo").setIndices(List.of("logs-*")).build();
        PreviewDatafeedAction.Request request = new PreviewDatafeedAction.Request("legacy_feed", (String) null, (String) null);

        assertThat(TransportPreviewDatafeedAction.isCrossProjectPreviewAllowed(request, persisted, true), is(false));
    }

    public void testIsCrossProjectPreviewAllowed_GivenPersistedWithEnvelopeAndCaller_ShouldReturnTrue() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("uiam_feed", "job_foo");
        builder.setIndices(List.of("logs-*"));
        builder.setCloudInternalCredential(new PersistedCloudCredential("id", randomCloudCredentialEncryptedData()));
        PreviewDatafeedAction.Request request = new PreviewDatafeedAction.Request("uiam_feed", (String) null, (String) null);

        assertThat(TransportPreviewDatafeedAction.isCrossProjectPreviewAllowed(request, builder.build(), true), is(true));
    }

    public void testIsCrossProjectPreviewAllowed_GivenInlinePreviewAndCaller_ShouldReturnTrue() {
        DatafeedConfig inline = new DatafeedConfig.Builder("inline_feed", "job_foo").setIndices(List.of("logs-*")).build();
        PreviewDatafeedAction.Request request = new PreviewDatafeedAction.Request(inline, null, null, null);

        assertThat(TransportPreviewDatafeedAction.isCrossProjectPreviewAllowed(request, inline, true), is(true));
    }

    public void testIsCrossProjectPreviewAllowed_GivenNoCallerCredential_ShouldReturnFalse() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("uiam_feed", "job_foo");
        builder.setIndices(List.of("logs-*"));
        builder.setCloudInternalCredential(new PersistedCloudCredential("id", randomCloudCredentialEncryptedData()));
        PreviewDatafeedAction.Request persistedRequest = new PreviewDatafeedAction.Request("uiam_feed", (String) null, (String) null);
        PreviewDatafeedAction.Request inlineRequest = new PreviewDatafeedAction.Request(builder.build(), null, null, null);

        assertThat(TransportPreviewDatafeedAction.isCrossProjectPreviewAllowed(persistedRequest, builder.build(), false), is(false));
        assertThat(TransportPreviewDatafeedAction.isCrossProjectPreviewAllowed(inlineRequest, builder.build(), false), is(false));
    }

    public void testWithCrossProjectModeIfEnabled_GivenCpsEnabled_ShouldEnableCrossProjectIndicesOptions() {
        assumeTrue("CPS feature flag must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("preview_cps_feed", "job_foo");
        builder.setIndices(Collections.singletonList("logs-*"));
        builder.setIndicesOptions(org.elasticsearch.action.support.IndicesOptions.STRICT_EXPAND_OPEN);
        CrossProjectModeDecider decider = new CrossProjectModeDecider(
            Settings.builder().put("serverless.cross_project.enabled", true).build()
        );

        DatafeedConfig result = DatafeedConfig.withCrossProjectModeIfEnabled(builder.build(), decider, true);

        assertThat(result.getIndicesOptions().resolveCrossProjectIndexExpression(), is(true));
    }

    public void testWithCrossProjectModeIfEnabled_FlagOffPreviewClearsProjectRouting() {
        assumeFalse("Run with -Des.ml_cross_project_feature_flag_enabled=false", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("preview_flag_off_feed", "job_foo");
        builder.setIndices(Collections.singletonList("logs-*"));
        builder.setProjectRouting("_alias:prod-*");
        CrossProjectModeDecider decider = new CrossProjectModeDecider(
            Settings.builder().put("serverless.cross_project.enabled", true).build()
        );

        DatafeedConfig previewConfig = TransportPreviewDatafeedAction.buildPreviewDatafeed(builder.build()).build();
        DatafeedConfig effective = DatafeedConfig.withCrossProjectModeIfEnabled(previewConfig, decider, true);

        assertThat(effective.getProjectRouting(), nullValue());
        assertThat(effective.getIndicesOptions().resolveCrossProjectIndexExpression(), is(false));
        assertThat(previewConfig.getProjectRouting(), equalTo("_alias:prod-*"));
    }

    public void testWithCrossProjectModeIfEnabled_GivenNoCallerCredential_DoesNotPromote() {
        assumeTrue("CPS feature flag must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("preview_no_cred_feed", "job_foo");
        builder.setIndices(Collections.singletonList("logs-*"));
        builder.setIndicesOptions(org.elasticsearch.action.support.IndicesOptions.STRICT_EXPAND_OPEN);
        CrossProjectModeDecider decider = new CrossProjectModeDecider(
            Settings.builder().put("serverless.cross_project.enabled", true).build()
        );

        DatafeedConfig result = DatafeedConfig.withCrossProjectModeIfEnabled(builder.build(), decider, false);

        assertThat(result.getIndicesOptions().resolveCrossProjectIndexExpression(), is(false));
    }

    public void testBuildDateNanosFieldCapsRequest_GivenCpsIndicesOptions_ShouldRequestResolvedTo() {
        assumeTrue("CPS feature flag must be enabled", CloudCredentialsExtension.ML_CROSS_PROJECT.isEnabled());
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("preview_cps_feed", "job_foo");
        builder.setIndices(List.of("local-*", "linked_project:remote-*"));
        builder.setIndicesOptions(org.elasticsearch.action.support.IndicesOptions.STRICT_EXPAND_OPEN);
        CrossProjectModeDecider decider = new CrossProjectModeDecider(
            Settings.builder().put("serverless.cross_project.enabled", true).build()
        );
        DatafeedConfig datafeed = DatafeedConfig.withCrossProjectModeIfEnabled(builder.build(), decider, true);
        assertThat(datafeed.getIndicesOptions().resolveCrossProjectIndexExpression(), is(true));

        FieldCapabilitiesRequest request = TransportPreviewDatafeedAction.buildDateNanosFieldCapsRequest(datafeed, "time");

        // Without includeResolvedTo, the coordinator's cross-project resolution validator runs against an empty
        // per-project map and trips a node-fatal assertion for explicitly qualified expressions.
        assertThat(request.includeResolvedTo(), is(true));
    }

    public void testBuildDateNanosFieldCapsRequest_GivenNonCpsIndicesOptions_ShouldNotRequestResolvedTo() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("preview_feed", "job_foo");
        builder.setIndices(Collections.singletonList("my_index"));

        FieldCapabilitiesRequest request = TransportPreviewDatafeedAction.buildDateNanosFieldCapsRequest(builder.build(), "time");

        assertThat(request.includeResolvedTo(), is(false));
        assertThat(request.indices(), equalTo(new String[] { "my_index" }));
    }

    public void testBuildPreviewDatafeed_GivenNoAggregations() {
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("no_aggs_feed", "job_foo");
        datafeed.setIndices(Collections.singletonList("my_index"));
        datafeed.setChunkingConfig(ChunkingConfig.newManual(TimeValue.timeValueHours(1)));

        DatafeedConfig previewDatafeed = TransportPreviewDatafeedAction.buildPreviewDatafeed(datafeed.build()).build();

        assertThat(previewDatafeed.getChunkingConfig(), equalTo(ChunkingConfig.newAuto()));
    }

    public void testBuildPreviewDatafeed_GivenAggregations() {
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("no_aggs_feed", "job_foo");
        datafeed.setIndices(Collections.singletonList("my_index"));
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        datafeed.setParsedAggregations(
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.histogram("time").interval(300000).subAggregation(maxTime).field("time"))
        );
        datafeed.setChunkingConfig(ChunkingConfig.newManual(TimeValue.timeValueHours(1)));

        DatafeedConfig previewDatafeed = TransportPreviewDatafeedAction.buildPreviewDatafeed(datafeed.build()).build();

        assertThat(previewDatafeed.getChunkingConfig(), not(equalTo(ChunkingConfig.newAuto())));
        assertThat(previewDatafeed.getChunkingConfig(), equalTo(datafeed.build().getChunkingConfig()));
    }

    public void testPreviewDatafeed_GivenEmptyStream() throws IOException {
        when(dataExtractor.next()).thenReturn(new DataExtractor.Result(SearchIntervalTests.createRandom(), Optional.empty(), List.of()));

        TransportPreviewDatafeedAction.previewDatafeed(dataExtractor, actionListener);

        assertThat(capturedResponse, equalTo("[]"));
        assertThat(capturedFailure, is(nullValue()));
        verify(dataExtractor).destroy();
    }

    public void testPreviewDatafeed_GivenNonEmptyStream() throws IOException {
        String streamAsString = "{\"a\":1, \"b\":2} {\"c\":3, \"d\":4}\n{\"e\":5, \"f\":6}";
        InputStream stream = new ByteArrayInputStream(streamAsString.getBytes(StandardCharsets.UTF_8));
        when(dataExtractor.next()).thenReturn(new DataExtractor.Result(SearchIntervalTests.createRandom(), Optional.of(stream), List.of()));

        TransportPreviewDatafeedAction.previewDatafeed(dataExtractor, actionListener);

        assertThat(capturedResponse, equalTo("[{\"a\":1, \"b\":2},{\"c\":3, \"d\":4},{\"e\":5, \"f\":6}]"));
        assertThat(capturedFailure, is(nullValue()));
        verify(dataExtractor).destroy();
    }

    public void testPreviewDatafeed_GivenFailure() throws IOException {
        doThrow(new RuntimeException("failed")).when(dataExtractor).next();

        TransportPreviewDatafeedAction.previewDatafeed(dataExtractor, actionListener);

        assertThat(capturedResponse, is(nullValue()));
        assertThat(capturedFailure.getMessage(), equalTo("failed"));
        verify(dataExtractor).destroy();
    }

    public void testTimeFieldIsDateNanos_GivenFieldAbsent_ReturnsFalse() {
        FieldCapabilitiesResponse response = FieldCapabilitiesResponse.empty();
        assertThat(TransportPreviewDatafeedAction.timeFieldIsDateNanos(response, "time"), is(false));
    }

    public void testTimeFieldIsDateNanos_GivenDateNanos_ReturnsTrue() {
        String timeField = "event_time";
        var caps = new FieldCapabilitiesBuilder(timeField, DateFieldMapper.DATE_NANOS_CONTENT_TYPE).build();
        Map<String, FieldCapabilities> byType = Map.of(DateFieldMapper.DATE_NANOS_CONTENT_TYPE, caps);
        FieldCapabilitiesResponse response = new FieldCapabilitiesResponse(Strings.EMPTY_ARRAY, Map.of(timeField, byType));
        assertThat(TransportPreviewDatafeedAction.timeFieldIsDateNanos(response, timeField), is(true));
    }

    public void testTimeFieldIsDateNanos_GivenDateOnly_ReturnsFalse() {
        String timeField = "event_time";
        var caps = new FieldCapabilitiesBuilder(timeField, DateFieldMapper.CONTENT_TYPE).build();
        Map<String, FieldCapabilities> byType = Map.of(DateFieldMapper.CONTENT_TYPE, caps);
        FieldCapabilitiesResponse response = new FieldCapabilitiesResponse(Strings.EMPTY_ARRAY, Map.of(timeField, byType));
        assertThat(TransportPreviewDatafeedAction.timeFieldIsDateNanos(response, timeField), is(false));
    }
}
