/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.lifecycle.ErrorEntry;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.downsample.Downsample;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.ClusterChangedEventUtils.indicesCreated;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * This test suite ensures that data stream lifecycle runtime tasks work correctly with security enabled, i.e., that the internal user for
 * data stream lifecycle has all requisite privileges to orchestrate the data stream lifecycle
 * This class focuses on the donwsampling execution.
 */
public class DataStreamLifecycleDownsamplingSecurityIT extends SecurityIntegTestCase {
    private static final Logger logger = LogManager.getLogger(DataStreamLifecycleDownsamplingSecurityIT.class);

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    public static final String FIELD_TIMESTAMP = "@timestamp";
    public static final String FIELD_DIMENSION_1 = "dimension_kw";
    public static final String FIELD_DIMENSION_2 = "dimension_long";
    public static final String FIELD_METRIC_COUNTER = "counter";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            LocalStateSecurity.class,
            DataStreamsPlugin.class,
            MapperExtrasPlugin.class,
            Wildcard.class,
            Downsample.class,
            AggregateMetricMapperPlugin.class,
            SystemDataStreamWithDownsamplingConfigurationPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        return settings.build();
    }

    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle:TRACE", reason = "debugging")
    public void testDownsamplingAuthorized() throws Exception {
        String dataStreamName = "metrics-foo";

        DataStreamLifecycle lifecycle = DataStreamLifecycle.newBuilder()
            .downsampling(
                new DataStreamLifecycle.Downsampling(
                    List.of(
                        new DataStreamLifecycle.Downsampling.Round(
                            TimeValue.timeValueMillis(0),
                            new DownsampleConfig(new DateHistogramInterval("5m"))
                        ),
                        new DataStreamLifecycle.Downsampling.Round(
                            TimeValue.timeValueSeconds(10),
                            new DownsampleConfig(new DateHistogramInterval("10m"))
                        )
                    )
                )
            )
            .build();

        setupDataStreamAndIngestDocs(
            client(),
            dataStreamName,
            "1986-01-08T23:40:53.384Z",
            "2022-01-08T23:40:53.384Z",
            lifecycle,
            10_000,
            "1990-09-09T18:00:00"
        );
        List<Index> backingIndices = getDataStreamBackingIndices(dataStreamName);
        String firstGenerationBackingIndex = backingIndices.get(0).getName();
        String firstRoundDownsamplingIndex = "downsample-5m-" + firstGenerationBackingIndex;
        String secondRoundDownsamplingIndex = "downsample-10m-" + firstGenerationBackingIndex;

        Set<String> witnessedDownsamplingIndices = new HashSet<>();
        clusterService().addListener(event -> {
            if (indicesCreated(event).contains(firstRoundDownsamplingIndex)
                || event.indicesDeleted().stream().anyMatch(index -> index.getName().equals(firstRoundDownsamplingIndex))) {
                witnessedDownsamplingIndices.add(firstRoundDownsamplingIndex);
            }
            if (indicesCreated(event).contains(secondRoundDownsamplingIndex)) {
                witnessedDownsamplingIndices.add(secondRoundDownsamplingIndex);
            }
        });

        // before we rollover we update the index template to remove the start/end time boundaries (they're there just to ease with
        // testing so DSL doesn't have to wait for the end_time to lapse)
        putTSDBIndexTemplate(client(), dataStreamName, null, null, lifecycle);
        client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)).actionGet();

        assertBusy(() -> {
            assertNoAuthzErrors();
            // first downsampling round
            assertThat(witnessedDownsamplingIndices.contains(firstRoundDownsamplingIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            assertNoAuthzErrors();
            assertThat(witnessedDownsamplingIndices.size(), is(2));
            assertThat(witnessedDownsamplingIndices.contains(firstRoundDownsamplingIndex), is(true));

            assertThat(witnessedDownsamplingIndices.contains(secondRoundDownsamplingIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            assertNoAuthzErrors();
            List<Index> dsBackingIndices = getDataStreamBackingIndices(dataStreamName);

            assertThat(dsBackingIndices.size(), is(2));
            String writeIndex = dsBackingIndices.get(1).getName();
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
            // the last downsampling round must remain in the data stream
            assertThat(dsBackingIndices.get(0).getName(), is(secondRoundDownsamplingIndex));
            assertThat(indexExists(firstGenerationBackingIndex), is(false));
            assertThat(indexExists(firstRoundDownsamplingIndex), is(false));
        }, 30, TimeUnit.SECONDS);
    }

    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle:TRACE", reason = "debugging")
    public void testSystemDataStreamConfigurationWithDownsampling() throws Exception {
        String dataStreamName = SystemDataStreamWithDownsamplingConfigurationPlugin.SYSTEM_DATA_STREAM_NAME;
        indexDocuments(client(), dataStreamName, 10_000, Instant.now().toEpochMilli());
        List<Index> backingIndices = getDataStreamBackingIndices(dataStreamName);
        String firstGenerationBackingIndex = backingIndices.get(0).getName();
        String secondRoundDownsamplingIndex = "downsample-10m-" + firstGenerationBackingIndex;

        Set<String> witnessedDownsamplingIndices = new HashSet<>();
        clusterService().addListener(event -> {
            if (indicesCreated(event).contains(secondRoundDownsamplingIndex)) {
                witnessedDownsamplingIndices.add(secondRoundDownsamplingIndex);
            }
        });

        DataStreamLifecycleService masterDataStreamLifecycleService = internalCluster().getCurrentMasterNodeInstance(
            DataStreamLifecycleService.class
        );
        try {
            // we can't update the index template backing a system data stream, so we run DSL "in the future"
            // this means that only one round of downsampling will execute due to an optimisation we have in DSL to execute the last
            // matching round
            masterDataStreamLifecycleService.setNowSupplier(() -> Instant.now().plus(50, ChronoUnit.DAYS).toEpochMilli());
            client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)).actionGet();

            assertBusy(() -> {
                assertNoAuthzErrors();
                assertThat(witnessedDownsamplingIndices.contains(secondRoundDownsamplingIndex), is(true));
            }, 30, TimeUnit.SECONDS);

            assertBusy(() -> {
                assertNoAuthzErrors();
                List<Index> dsBackingIndices = getDataStreamBackingIndices(dataStreamName);

                assertThat(dsBackingIndices.size(), is(2));
                String writeIndex = dsBackingIndices.get(1).getName();
                assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
                // the last downsampling round must remain in the data stream
                assertThat(dsBackingIndices.get(0).getName(), is(secondRoundDownsamplingIndex));
            }, 30, TimeUnit.SECONDS);
        } finally {
            // restore a real nowSupplier so other tests running against this cluster succeed
            masterDataStreamLifecycleService.setNowSupplier(() -> Instant.now().toEpochMilli());
        }
    }

    private Map<String, String> collectErrorsFromStoreAsMap() {
        Iterable<DataStreamLifecycleService> lifecycleServices = internalCluster().getInstances(DataStreamLifecycleService.class);
        Map<String, String> indicesAndErrors = new HashMap<>();
        for (DataStreamLifecycleService lifecycleService : lifecycleServices) {
            DataStreamLifecycleErrorStore errorStore = lifecycleService.getErrorStore();
            Set<String> allIndices = errorStore.getAllIndices();
            for (var index : allIndices) {
                ErrorEntry error = errorStore.getError(index);
                if (error != null) {
                    indicesAndErrors.put(index, error.error());
                }
            }
        }
        return indicesAndErrors;
    }

    private List<Index> getDataStreamBackingIndices(String dataStreamName) {
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { dataStreamName }
        );
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
        return getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
    }

    private void assertNoAuthzErrors() {
        var indicesAndErrors = collectErrorsFromStoreAsMap();
        for (var entry : indicesAndErrors.entrySet()) {
            assertThat(
                "unexpected authz error for index [" + entry.getKey() + "] with error message [" + entry.getValue() + "]",
                entry.getValue(),
                not(anyOf(containsString("security_exception"), containsString("unauthorized for user [_data_stream_lifecycle]")))
            );
        }
    }

    private void setupDataStreamAndIngestDocs(
        Client client,
        String dataStreamName,
        @Nullable String startTime,
        @Nullable String endTime,
        DataStreamLifecycle lifecycle,
        int docCount,
        String firstDocTimestamp
    ) throws IOException {
        putTSDBIndexTemplate(client, dataStreamName + "*", startTime, endTime, lifecycle);
        long startTimestamp = LocalDateTime.parse(firstDocTimestamp).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        indexDocuments(client, dataStreamName, docCount, startTimestamp);
    }

    private void putTSDBIndexTemplate(
        Client client,
        String pattern,
        @Nullable String startTime,
        @Nullable String endTime,
        DataStreamLifecycle lifecycle
    ) throws IOException {
        Settings.Builder settings = indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1));
        if (Strings.hasText(startTime)) {
            settings.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), startTime);
        }

        if (Strings.hasText(endTime)) {
            settings.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), endTime);
        }
        CompressedXContent mapping = getTSDBMappings();
        putComposableIndexTemplate(client, "id1", mapping, List.of(pattern), settings.build(), null, lifecycle);
    }

    private static CompressedXContent getTSDBMappings() throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");
        mapping.startObject(FIELD_TIMESTAMP).field("type", "date").endObject();

        mapping.startObject(FIELD_DIMENSION_1).field("type", "keyword").field("time_series_dimension", true).endObject();
        mapping.startObject(FIELD_DIMENSION_2).field("type", "long").field("time_series_dimension", true).endObject();

        mapping.startObject(FIELD_METRIC_COUNTER)
            .field("type", "double") /* numeric label indexed as a metric */
            .field("time_series_metric", "counter")
            .endObject();

        mapping.endObject().endObject().endObject();
        return CompressedXContent.fromJSON(Strings.toString(mapping));
    }

    private void putComposableIndexTemplate(
        Client client,
        String id,
        @Nullable CompressedXContent mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataStreamLifecycle lifecycle
    ) {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(Template.builder().settings(settings).mappings(mappings).lifecycle(lifecycle))
                .metadata(metadata)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client.execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

    private void indexDocuments(Client client, String dataStreamName, int docCount, long startTime) {
        final Supplier<XContentBuilder> sourceSupplier = () -> {
            final String ts = randomDateForInterval(new DateHistogramInterval("1s"), startTime);
            double counterValue = DATE_FORMATTER.parseMillis(ts);
            final List<String> dimensionValues = new ArrayList<>(5);
            for (int j = 0; j < randomIntBetween(1, 5); j++) {
                dimensionValues.add(randomAlphaOfLength(6));
            }
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field(FIELD_TIMESTAMP, ts)
                    .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                    .field(FIELD_DIMENSION_2, randomIntBetween(1, 10))
                    .field(FIELD_METRIC_COUNTER, counterValue)
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        bulkIndex(client, dataStreamName, sourceSupplier, docCount);
    }

    private String randomDateForInterval(final DateHistogramInterval interval, final long startTime) {
        long endTime = startTime + 10 * interval.estimateMillis();
        return randomDateForRange(startTime, endTime);
    }

    private String randomDateForRange(long start, long end) {
        return DATE_FORMATTER.formatMillis(randomLongBetween(start, end));
    }

    private void bulkIndex(Client client, String dataStreamName, Supplier<XContentBuilder> docSourceSupplier, int docCount) {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docCount; i++) {
            IndexRequest indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
            XContentBuilder source = docSourceSupplier.get();
            indexRequest.source(source);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        int duplicates = 0;
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (response.isFailed()) {
                if (response.getFailure().getCause() instanceof VersionConflictEngineException) {
                    // A duplicate event was created by random generator. We should not fail for this
                    // reason.
                    logger.debug("-> failed to insert a duplicate: [{}]", response.getFailureMessage());
                    duplicates++;
                } else {
                    throw new ElasticsearchException("Failed to index data: " + bulkResponse.buildFailureMessage());
                }
            }
        }
        int docsIndexed = docCount - duplicates;
        logger.info("-> Indexed [{}] documents. Dropped [{}] duplicates.", docsIndexed, duplicates);
    }

    public static class SystemDataStreamWithDownsamplingConfigurationPlugin extends Plugin implements SystemIndexPlugin {

        public static final DataStreamLifecycle LIFECYCLE = DataStreamLifecycle.newBuilder()
            .downsampling(
                new DataStreamLifecycle.Downsampling(
                    List.of(
                        new DataStreamLifecycle.Downsampling.Round(
                            TimeValue.timeValueMillis(0),
                            new DownsampleConfig(new DateHistogramInterval("5m"))
                        ),
                        new DataStreamLifecycle.Downsampling.Round(
                            TimeValue.timeValueSeconds(10),
                            new DownsampleConfig(new DateHistogramInterval("10m"))
                        )
                    )
                )
            )
            .build();
        static final String SYSTEM_DATA_STREAM_NAME = ".fleet-actions-results";

        @Override
        public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
            Settings.Builder settings = indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1));

            try {
                return List.of(
                    new SystemDataStreamDescriptor(
                        SYSTEM_DATA_STREAM_NAME,
                        "a system data stream for testing",
                        SystemDataStreamDescriptor.Type.EXTERNAL,
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(SYSTEM_DATA_STREAM_NAME))
                            .template(Template.builder().settings(settings).mappings(getTSDBMappings()).lifecycle(LIFECYCLE))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .build(),
                        Map.of(),
                        Collections.singletonList("test"),
                        new ExecutorNames(
                            ThreadPool.Names.SYSTEM_CRITICAL_READ,
                            ThreadPool.Names.SYSTEM_READ,
                            ThreadPool.Names.SYSTEM_WRITE
                        )
                    )
                );
            } catch (IOException e) {
                throw new RuntimeException("Unable to create system data stream descriptor", e);
            }
        }

        @Override
        public String getFeatureName() {
            return SystemDataStreamWithDownsamplingConfigurationPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A plugin for testing the data stream lifecycle runtime actions on system data streams";
        }
    }
}
