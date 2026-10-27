/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.Exemplar;
import io.opentelemetry.proto.metrics.v1.Metric;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OTLPMetricsTransportActionTests extends AbstractOTLPTransportActionTests {

    private OTLPMetricsTransportAction metricsAction;
    private ClusterSettings clusterSettings;

    @Override
    protected AbstractOTLPTransportAction createAction() {
        metricsAction = createMetricsAction(Settings.EMPTY);
        return metricsAction;
    }

    private OTLPMetricsTransportAction createMetricsAction(Settings settings) {
        return createMetricsAction(settings, ProjectMetadata.builder(ProjectId.DEFAULT).build());
    }

    private OTLPMetricsTransportAction createMetricsAction(Settings settings, ProjectMetadata projectMetadata) {
        ClusterService clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING, BatchIndexingEnabled.BATCH_INDEXING, XPackSettings.METRIC_EXEMPLARS_ENABLED)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().projectMetadata(Map.of(ProjectId.DEFAULT, projectMetadata)).build())
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        return new OTLPMetricsTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ThreadPool.class),
            client,
            clusterService,
            BigArrays.NON_RECYCLING_INSTANCE,
            settings
        );
    }

    @Override
    protected OTLPActionRequest createRequestWithData() {
        return createMetricsRequest(createMetric());
    }

    @Override
    protected OTLPActionRequest createEmptyRequest() {
        return createMetricsRequest();
    }

    @Override
    protected boolean parseHasPartialSuccess(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportMetricsServiceResponse.parseFrom(responseBytes).hasPartialSuccess();
    }

    @Override
    protected long parseRejectedCount(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportMetricsServiceResponse.parseFrom(responseBytes).getPartialSuccess().getRejectedDataPoints();
    }

    @Override
    protected String parseErrorMessage(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportMetricsServiceResponse.parseFrom(responseBytes).getPartialSuccess().getErrorMessage();
    }

    @Override
    protected String dataStreamType() {
        return "metrics";
    }

    // --- metrics-specific tests ---

    public void testMappingHintsSettingsUpdate() throws Exception {
        assertThat(metricsAction.defaultMappingHints, equalTo(MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM));
        assertThat(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING.isDynamic(), equalTo(true));

        clusterSettings.applySettings(Settings.builder().put(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING.getKey(), "histogram").build());
        assertThat(metricsAction.defaultMappingHints, equalTo(MappingHints.DEFAULT_TDIGEST));

        clusterSettings.applySettings(
            Settings.builder().put(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING.getKey(), "exponential_histogram").build()
        );
        assertThat(metricsAction.defaultMappingHints, equalTo(MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM));
    }

    public void testExemplarIngestionFollowsClusterSetting() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "ms",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        metricsAction.prepareBulkRequest(createMetricsRequest(metric), bulkRequestBuilder);

        var requests = bulkRequestBuilder.request().requests();
        assertThat(requests, hasSize(2));
        IndexRequest exemplarRequest = (IndexRequest) requests.get(1);
        assertThat(exemplarRequest.index(), equalTo("exemplars-generic.otel-default"));
        assertThat(exemplarRequest.getDynamicTemplates(), equalTo(Map.of()));
        assertThat(exemplarRequest.getDynamicTemplateParams(), equalTo(Map.of()));

        clusterSettings.applySettings(Settings.builder().put(XPackSettings.METRIC_EXEMPLARS_ENABLED.getKey(), false).build());
        bulkRequestBuilder = new BulkRequestBuilder(client);
        metricsAction.prepareBulkRequest(createMetricsRequest(metric), bulkRequestBuilder);
        assertThat(bulkRequestBuilder.request().requests(), hasSize(1));
    }

    public void testExemplarDocumentsFollowMetricDocuments() throws Exception {
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "ms",
            List.of(
                OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(OtlpUtils.createLongExemplar(1_000_000L, 42L))),
                OtlpUtils.createDoubleDataPoint(4_000_000L, 0, List.of(), List.of(OtlpUtils.createLongExemplar(3_000_000L, 43L)))
            )
        );
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        AbstractOTLPTransportAction.ProcessingContext context = metricsAction.prepareBulkRequest(
            createMetricsRequest(metric),
            bulkRequestBuilder
        );

        var requests = bulkRequestBuilder.request().requests();
        assertThat(requests, hasSize(4));
        assertThat(requests.get(0).index(), equalTo("metrics-generic.otel-default"));
        assertThat(requests.get(1).index(), equalTo("metrics-generic.otel-default"));
        assertThat(requests.get(2).index(), equalTo("exemplars-generic.otel-default"));
        assertThat(requests.get(3).index(), equalTo("exemplars-generic.otel-default"));
        assertTrue(context.isPrimaryTelemetryDoc(0));
        assertTrue(context.isPrimaryTelemetryDoc(1));
        assertFalse(context.isPrimaryTelemetryDoc(2));
        assertFalse(context.isPrimaryTelemetryDoc(3));
    }

    public void testExemplarWithoutTargetProducesWarning() throws Exception {
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(
                OtlpUtils.createDoubleDataPoint(
                    2_000_000L,
                    0,
                    List.of(keyValue("elasticsearch.index", "custom-index")),
                    List.of(OtlpUtils.createLongExemplar(1_000_000L, 42L))
                )
            )
        );

        OTLPActionResponse response = executeRequest(
            createMetricsRequest(metric),
            new BulkResponse(new BulkItemResponse[] { successResponse() }, 0)
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(0L));
        assertThat(
            parseErrorMessage(responseBytes),
            equalTo("1 exemplars were dropped because no exemplar data stream can be derived from an explicit index target.\n")
        );
    }

    public void testExemplarWithoutValueProducesWarning() throws Exception {
        Exemplar exemplar = Exemplar.newBuilder().setTimeUnixNano(1_000_000L).build();
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );

        OTLPActionResponse response = executeRequest(
            createMetricsRequest(metric),
            new BulkResponse(new BulkItemResponse[] { successResponse() }, 0)
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(0L));
        assertThat(parseErrorMessage(responseBytes), equalTo("1 exemplars were dropped because they have no value.\n"));
    }

    public void testDuplicateExemplarWarning() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar, exemplar)))
        );

        OTLPActionResponse response = executeRequest(
            createMetricsRequest(metric),
            new BulkResponse(new BulkItemResponse[] { successResponse(), successResponse() }, 0)
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(0L));
        assertThat(parseErrorMessage(responseBytes), equalTo("1 exemplars were dropped due to duplicate timestamps and series identity"));
    }

    public void testExemplarFailureStoreRedirectAndDuplicateWarnings() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar, exemplar)))
        );

        OTLPActionResponse response = executeRequest(
            createMetricsRequest(metric),
            new BulkResponse(new BulkItemResponse[] { successResponse(), failureStoreUsedResponse() }, 0)
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(0L));
        assertThat(
            parseErrorMessage(responseBytes),
            equalTo(
                "Redirected 1 exemplar documents to the failure store.\n"
                    + "1 exemplars were dropped due to duplicate timestamps and series identity"
            )
        );
    }

    public void testExemplarIndexingFailureDoesNotRejectDataPoint() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );

        OTLPActionResponse response = executeRequest(
            createMetricsRequest(metric),
            new BulkResponse(
                new BulkItemResponse[] {
                    successResponse(),
                    bulkItemFailure("exemplars-generic.otel-default", RestStatus.FORBIDDEN, "unauthorized") },
                0
            )
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(0L));
        assertThat(
            parseErrorMessage(responseBytes),
            equalTo("Failed to index 1 exemplar documents. Sample error message: java.lang.RuntimeException: unauthorized\n")
        );
    }

    /**
     * An already indexed exemplar must not hide rejected data points when multiple metrics share a failed document.
     */
    public void testExemplarConflictDoesNotSuppressAllDataPointsRejected() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric firstMetric = OtlpUtils.createGaugeMetric(
            "first.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );
        Metric secondMetric = OtlpUtils.createGaugeMetric(
            "second.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of()))
        );
        OTLPActionRequest request = createMetricsRequest(firstMetric, secondMetric);
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);
        AbstractOTLPTransportAction.ProcessingContext context = createAction().prepareBulkRequest(request, bulkRequestBuilder);
        assertThat(context.totalItems(), equalTo(2));
        assertThat(bulkRequestBuilder.numberOfActions(), equalTo(2));
        assertTrue(context.isPrimaryTelemetryDoc(0));
        assertFalse(context.isPrimaryTelemetryDoc(1));

        OTLPActionResponse response = executeRequest(
            request,
            new BulkResponse(
                new BulkItemResponse[] {
                    bulkItemFailure("metrics-generic.otel-default", RestStatus.BAD_REQUEST, "bad request"),
                    bulkItemFailure("exemplars-generic.otel-default", RestStatus.CONFLICT, "version conflict") },
                0
            )
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(2L));
        assertThat(parseErrorMessage(responseBytes), containsString("returned status [BAD_REQUEST] for 1 documents"));
        assertThat(
            parseErrorMessage(responseBytes),
            containsString("Skipped 1 duplicate documents that were already indexed with the same id.\n")
        );
    }

    public void testExemplarConflictIsReportedAsDuplicate() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );

        OTLPActionResponse response = executeRequest(
            createMetricsRequest(metric),
            new BulkResponse(
                new BulkItemResponse[] {
                    bulkItemFailure("metrics-generic.otel-default", RestStatus.CONFLICT, "version conflict"),
                    bulkItemFailure("exemplars-generic.otel-default", RestStatus.CONFLICT, "version conflict") },
                0
            )
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(0L));
        assertThat(
            parseErrorMessage(responseBytes),
            equalTo("Skipped 2 duplicate documents that were already indexed with the same id.\n")
        );
    }

    public void testSameTimestampExemplarsForDifferentMetricsAreNotDuplicates() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric firstMetric = OtlpUtils.createGaugeMetric(
            "first.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );
        Metric secondMetric = OtlpUtils.createGaugeMetric(
            "second.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        metricsAction.prepareBulkRequest(createMetricsRequest(firstMetric, secondMetric), bulkRequestBuilder);

        var requests = bulkRequestBuilder.request().requests();
        assertThat(requests, hasSize(3));
        IndexRequest firstExemplarRequest = (IndexRequest) requests.get(1);
        IndexRequest secondExemplarRequest = (IndexRequest) requests.get(2);
        assertNotEquals(firstExemplarRequest.tsid(), secondExemplarRequest.tsid());
    }

    public void testBatchIndexingGateForExemplars() throws Exception {
        assumeTrue("requires batch indexing", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        clusterSettings.applySettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true).build());

        ProjectMetadata projectMetadata = batchIndexingProject("metrics-generic.otel-default");

        List<DataPointGroupingContext.DataPointGroup> groups = collectGroups(
            ExportMetricsServiceRequest.parseFrom(createMetricsRequest(createMetric()).getRequest().streamInput())
        );
        assertTrue(metricsAction.canUseBatchIndexing(true, projectMetadata, groups));

        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metricWithExemplar = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );
        List<DataPointGroupingContext.DataPointGroup> groupsWithExemplars = collectGroups(
            ExportMetricsServiceRequest.parseFrom(createMetricsRequest(metricWithExemplar).getRequest().streamInput())
        );

        assertFalse(metricsAction.canUseBatchIndexing(true, projectMetadata, groupsWithExemplars));
    }

    public void testAttributeFanoutReturns413() {
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_CONTENT_LENGTH.getKey(), "1kb")
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_EXPANDED_CONTENT_LENGTH.getKey(), "10kb")
            .build();
        OTLPMetricsTransportAction action = createMetricsAction(settings);

        // Distinct data-point attributes force separate documents; a large resource attribute is copied into each.
        String largeValue = "x".repeat(1024);
        List<Metric> metrics = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            Metric metric = OtlpUtils.createGaugeMetric(
                "test.metric",
                "",
                List.of(OtlpUtils.createDoubleDataPoint(i, i, List.of(keyValue("series", String.valueOf(i)))))
            );
            metrics.add(metric);
            // Grouping discards duplicates, so only 15 of the 30 received points contribute to the size estimate.
            metrics.add(metric);
        }
        OTLPActionRequest request = new OTLPActionRequest(
            new BytesArray(
                OtlpUtils.createMetricsRequest(
                    List.of(keyValue("resource.large", largeValue), keyValue("service.name", "test-service")),
                    metrics
                ).toByteArray()
            )
        );

        @SuppressWarnings("unchecked")
        ActionListener<OTLPActionResponse> responseListener = mock(ActionListener.class);
        action.doExecute(null, request, responseListener);

        ArgumentCaptor<Exception> exception = ArgumentCaptor.forClass(Exception.class);
        verify(responseListener).onFailure(exception.capture());
        assertThat(ExceptionsHelper.status(exception.getValue()), equalTo(RestStatus.REQUEST_ENTITY_TOO_LARGE));
        assertThat(exception.getValue().getMessage(), containsString("expanded content would exceed limit"));
        assertThat(exception.getValue().getMessage(), containsString("[15] telemetry items selected for conversion"));
        assertThat(exception.getValue().getMessage(), containsString("Estimated expanded size of the full request"));
        verify(client, never()).execute(any(), any(), any());
    }

    /**
     * A data-point group whose resource carries an ARRAY attribute cannot be expressed as scalar ESCF
     * columns. The transport action now discovers this optimistically — while actually writing the row,
     * not via an upfront eligibility scan — and falls all the way back to the doc-mode (XContent) path for
     * the whole request rather than throwing.
     */
    public void testEscfFallsBackToDocModeForArrayResourceAttribute() throws Exception {
        assumeTrue("requires batch indexing", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        String target = "metrics-generic.otel-default";
        OTLPMetricsTransportAction action = createMetricsAction(Settings.EMPTY, batchIndexingProject(target));
        clusterSettings.applySettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true).build());

        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                OtlpUtils.createResourceMetrics(
                    List.of(keyValue("service.name", "svc"), keyValue("tags", "a", "b")), // ARRAY attribute
                    List.of(
                        OtlpUtils.createScopeMetrics(
                            "s",
                            "1",
                            List.of(OtlpUtils.createGaugeMetric("cpu", "1", List.of(OtlpUtils.createDoubleDataPoint(0))))
                        )
                    )
                )
            )
            .build();

        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);
        action.prepareBulkRequest(new OTLPActionRequest(new BytesArray(request.toByteArray())), bulkRequestBuilder);

        BulkRequest bulkRequest = bulkRequestBuilder.request();
        assertFalse("ESCF batch must not be registered once a non-scalar attribute is found", bulkRequest.hasPreBuiltBatches());
        var requests = bulkRequest.requests();
        assertThat(requests, hasSize(1));
        assertThat(requests.get(0).index(), equalTo(target));
    }

    private static List<DataPointGroupingContext.DataPointGroup> collectGroups(ExportMetricsServiceRequest request) throws Exception {
        DataPointGroupingContext ctx = new DataPointGroupingContext(
            new BufferedByteStringAccessor(),
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        ctx.groupDataPoints(request);
        List<DataPointGroupingContext.DataPointGroup> groups = new ArrayList<>();
        ctx.consume(groups::add);
        return groups;
    }

    // --- helpers ---

    private static OTLPActionRequest createMetricsRequest(Metric... metrics) {
        return new OTLPActionRequest(
            new BytesArray(
                ExportMetricsServiceRequest.newBuilder()
                    .addResourceMetrics(
                        OtlpUtils.createResourceMetrics(
                            List.of(keyValue("service.name", "test-service")),
                            List.of(OtlpUtils.createScopeMetrics("test", "1.0.0", List.of(metrics)))
                        )
                    )
                    .build()
                    .toByteArray()
            )
        );
    }

    private static Metric createMetric() {
        return OtlpUtils.createGaugeMetric("test.metric", "", List.of(OtlpUtils.createDoubleDataPoint(0)));
    }

    private static ProjectMetadata batchIndexingProject(String target) {
        return timeSeriesProject(Settings.builder().put(IndexSettings.TIME_SERIES_BATCH_INDEXING.getKey(), true).build(), target);
    }

    /** A project with one time series data stream per target, each with one backing index created with {@code extraSettings}. */
    private static ProjectMetadata timeSeriesProject(Settings extraSettings, String... targets) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(ProjectId.DEFAULT);
        Map<String, DataStream> dataStreams = new HashMap<>();
        for (String target : targets) {
            Index index = new Index(".ds-" + target + "-000001", randomUUID());
            IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
                .settings(
                    settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexSettings.MODE.getKey(), "time_series")
                        .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "1969-01-01T00:00:00Z")
                        .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "1971-01-01T00:00:00Z")
                        .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dimension")
                        .put(extraSettings)
                )
                .build();
            project.put(indexMetadata, true);
            dataStreams.put(target, DataStream.builder(target, List.of(index)).setIndexMode(IndexMode.TIME_SERIES).build());
        }
        return project.dataStreams(dataStreams, Map.of()).build();
    }

    /**
     * A project with the OTel metrics and exemplars index templates, where the tsid prefixed dimensions come from a component
     * template as in the real templates, and no data streams yet.
     */
    private static ProjectMetadata otelTemplatesProject(Settings componentSettings) {
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .put("metrics-otel@settings", new ComponentTemplate(new Template(componentSettings, null, null), null, null))
            .put("metrics-otel@template", otelTemplate("metrics-*.otel-*"))
            .put("exemplars-otel@template", otelTemplate("exemplars-*.otel-*"))
            .build();
    }

    private static ComposableIndexTemplate otelTemplate(String indexPattern) {
        return ComposableIndexTemplate.builder()
            .indexPatterns(List.of(indexPattern))
            .componentTemplates(List.of("metrics-otel@settings"))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .priority(120L)
            .build();
    }

    /** The metric and exemplar index requests the action prepares for one metric with one exemplar. */
    private List<IndexRequest> prepareMetricWithExemplar(OTLPMetricsTransportAction action) throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);
        action.prepareBulkRequest(createMetricsRequest(metric), bulkRequestBuilder);
        List<IndexRequest> requests = new ArrayList<>();
        for (var request : bulkRequestBuilder.request().requests()) {
            requests.add((IndexRequest) request);
        }
        assertThat(requests, hasSize(2));
        assertThat(requests.get(0).index(), equalTo("metrics-generic.otel-default"));
        assertThat(requests.get(1).index(), equalTo("exemplars-generic.otel-default"));
        return requests;
    }

    /**
     * Before a data stream exists, the pre-set tsids follow the layout of the index template that will create it, so that they
     * equal the tsids its backing index computes for the same series once it exists. Both the metric and the exemplar index
     * requests are covered, since exemplars share the tsid of their series.
     */
    public void testTsidLayoutResolvedFromTemplateBeforeDataStreamExists() throws Exception {
        Settings prefixedDimensions = Settings.builder()
            .putList(IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.getKey(), "resource.attributes.*")
            .build();
        List<IndexRequest> fromTemplate = prepareMetricWithExemplar(
            createMetricsAction(Settings.EMPTY, otelTemplatesProject(prefixedDimensions))
        );
        List<IndexRequest> fromIndex = prepareMetricWithExemplar(
            createMetricsAction(
                Settings.EMPTY,
                timeSeriesProject(prefixedDimensions, "metrics-generic.otel-default", "exemplars-generic.otel-default")
            )
        );
        List<IndexRequest> fromDefaults = prepareMetricWithExemplar(
            createMetricsAction(Settings.EMPTY, ProjectMetadata.builder(ProjectId.DEFAULT).build())
        );
        List<IndexRequest> fromTemplateWithoutSetting = prepareMetricWithExemplar(
            createMetricsAction(Settings.EMPTY, otelTemplatesProject(Settings.EMPTY))
        );
        for (int i = 0; i < 2; i++) {
            assertThat(fromTemplate.get(i).tsid(), equalTo(fromIndex.get(i).tsid()));
            assertThat(fromTemplate.get(i).tsid(), not(equalTo(fromDefaults.get(i).tsid())));
            assertThat(fromTemplateWithoutSetting.get(i).tsid(), equalTo(fromDefaults.get(i).tsid()));
        }
    }

    public void testPreBuiltBatchReleasedWhenBulkFails() {
        assumeTrue("requires batch indexing", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        OTLPMetricsTransportAction action = createMetricsAction(Settings.EMPTY, batchIndexingProject("metrics-generic.otel-default"));
        clusterSettings.applySettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true).build());
        ArgumentCaptor<BulkRequest> bulkRequest = ArgumentCaptor.forClass(BulkRequest.class);
        ArgumentCaptor<ActionListener<BulkResponse>> bulkListener = ArgumentCaptor.captor();
        doNothing().when(client).execute(any(), bulkRequest.capture(), bulkListener.capture());
        @SuppressWarnings("unchecked")
        ActionListener<OTLPActionResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);

        action.doExecute(null, createMetricsRequest(createMetric()), responseListener);
        assertTrue(bulkRequest.getValue().hasPreBuiltBatches());

        bulkListener.getValue().onFailure(new IllegalStateException("bulk rejected"));
        assertFalse(bulkRequest.getValue().hasPreBuiltBatches());
        verify(responseListener).onFailure(any());
    }

    public void testPreBuiltBatchReleasedWhenBulkExecuteThrows() {
        assumeTrue("requires batch indexing", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        OTLPMetricsTransportAction action = createMetricsAction(Settings.EMPTY, batchIndexingProject("metrics-generic.otel-default"));
        clusterSettings.applySettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true).build());
        ArgumentCaptor<BulkRequest> bulkRequest = ArgumentCaptor.forClass(BulkRequest.class);
        doThrow(new IllegalStateException("bulk rejected")).when(client).execute(any(), bulkRequest.capture(), any());
        @SuppressWarnings("unchecked")
        ActionListener<OTLPActionResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);

        action.doExecute(null, createMetricsRequest(createMetric()), responseListener);

        assertFalse(bulkRequest.getValue().hasPreBuiltBatches());
        verify(responseListener).onFailure(any());
    }
}
