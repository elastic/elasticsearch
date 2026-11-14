/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.stats.MappingVisitor;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM;
import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_METRIC_PARAM;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

/**
 * Base test case for downsampling integration tests. It provides helper methods to:
 * - set up templates and data streams
 * - index documents
 * - to assert the correctness of mapping, settings etc.
 */
public abstract class DownsamplingIntegTestCase extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(DownsamplingIntegTestCase.class);
    static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    static final String FIELD_TIMESTAMP = "@timestamp";
    static final String FIELD_DIMENSION_KEYWORD = "dimension_kw";
    static final String FIELD_DIMENSION_LONG = "dimension_long";
    static final String FIELD_METRIC_COUNTER_DOUBLE = "counter";
    static final String FIELD_METRIC_GAUGE_DOUBLE = "gauge";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            DataStreamsPlugin.class,
            LocalStateCompositeXPackPlugin.class,
            Downsample.class,
            AggregateMetricMapperPlugin.class,
            EncryptionPlugin.class,
            EsqlPlugin.class,
            AnalyticsPlugin.class
        );
    }

    /**
     * Sets up a TSDB data stream and ingests the specified number of documents
     * @return the count of indexed documents
     */
    public int setupTSDBDataStreamAndIngestDocs(
        String dataStreamName,
        @Nullable String startTime,
        @Nullable String endTime,
        DataStreamLifecycle.Template lifecycle,
        int docCount,
        String firstDocTimestamp
    ) throws IOException {
        putTSDBIndexTemplate(dataStreamName + "*", startTime, endTime, lifecycle);
        return indexDocuments(dataStreamName, docCount, firstDocTimestamp);
    }

    /**
     * Creates an index template that will create TSDB composable templates
     */
    public void putTSDBIndexTemplate(
        String pattern,
        @Nullable String startTime,
        @Nullable String endTime,
        DataStreamLifecycle.Template lifecycle
    ) throws IOException {
        Settings.Builder settings = indexSettings(1, 0).putList(
            IndexMetadata.INDEX_ROUTING_PATH.getKey(),
            List.of(FIELD_DIMENSION_KEYWORD)
        );

        if (Strings.hasText(startTime)) {
            settings.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), startTime);
        }

        if (Strings.hasText(endTime)) {
            settings.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), endTime);
        }

        String mappingString = String.format(Locale.ROOT, """
            {
              "properties": {
                "@timestamp": {
                  "type": "date"
                },
                "%s": {
                  "type": "keyword",
                  "time_series_dimension": true
                },
                "%s": {
                  "type": "long",
                  "time_series_dimension": true
                },
                "%s": {
                  "type": "double",
                  "time_series_metric": "counter"
                }
              }
            }""", FIELD_DIMENSION_KEYWORD, FIELD_DIMENSION_LONG, FIELD_METRIC_COUNTER_DOUBLE);

        putTSDBIndexTemplate("id1", List.of(pattern), settings.build(), mappingString, lifecycle, null);
    }

    void putTSDBIndexTemplate(
        String id,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable String mappingString,
        @Nullable DataStreamLifecycle.Template lifecycle,
        @Nullable Map<String, Object> metadata
    ) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        if (settings != null) {
            settingsBuilder.put(settings);
        }
        // Ensure it will be a TSDB data stream
        settingsBuilder.put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES);
        CompressedXContent mappings = mappingString == null ? null : CompressedXContent.fromJSON(mappingString);
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(Template.builder().settings(settingsBuilder).mappings(mappings).lifecycle(lifecycle))
                .metadata(metadata)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));
    }

    /**
     * Creates and indexes the specified number of documents using the docSource supplier.
     * @return the count of indexed documents
     */
    int bulkIndex(String dataStreamName, Supplier<XContentBuilder> docSourceSupplier, int docCount) {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
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
        return docsIndexed;
    }

    int indexDocuments(String dataStreamName, int docCount, String firstDocTimestamp) {
        final List<String> dimensionValues = new ArrayList<>(5);
        for (int j = 0; j < randomIntBetween(1, 5); j++) {
            dimensionValues.add(randomAlphaOfLength(6));
        }
        AtomicLong startTime = new AtomicLong(LocalDateTime.parse(firstDocTimestamp).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
        final Supplier<XContentBuilder> sourceSupplier = () -> {
            final String ts = randomDateForInterval(new DateHistogramInterval("1s"), startTime.get());
            startTime.set(startTime.get() + randomIntBetween(0, 30) * 1000L);
            double counterValue = DATE_FORMATTER.parseMillis(ts);
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field(FIELD_TIMESTAMP, ts)
                    .field(FIELD_DIMENSION_KEYWORD, randomFrom(dimensionValues))
                    .field(FIELD_DIMENSION_LONG, randomIntBetween(1, 10))
                    .field(FIELD_METRIC_COUNTER_DOUBLE, counterValue)
                    .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        return bulkIndex(dataStreamName, sourceSupplier, docCount);
    }

    String randomDateForInterval(final DateHistogramInterval interval, final long startTime) {
        long endTime = startTime + 10 * interval.estimateMillis();
        return randomDateForRange(startTime, endTime);
    }

    String randomDateForRange(long start, long end) {
        return DATE_FORMATTER.formatMillis(randomLongBetween(start, end));
    }

    /**
     * Currently we assert the correctness of metrics and dimensions. The assertions can be extended when needed.
     */
    @SuppressWarnings("unchecked")
    void assertDownsampleIndexFieldsAndDimensions(String sourceIndex, String downsampleIndex, DownsampleConfig config) throws Exception {
        GetIndexResponse getIndexResponse = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT)
            .setIndices(sourceIndex, downsampleIndex)
            .get();
        assertThat(getIndexResponse.indices(), arrayContaining(sourceIndex, downsampleIndex));

        // Retrieve field information for the metric fields
        final Map<String, Object> sourceIndexMappings = getIndexResponse.mappings().get(sourceIndex).getSourceAsMap();
        final Map<String, Object> downsampleIndexMappings = getIndexResponse.mappings().get(downsampleIndex).getSourceAsMap();

        final MapperService mapperService = getMapperServiceForIndex(sourceIndex);
        final CompressedXContent sourceIndexCompressedXContent = new CompressedXContent(sourceIndexMappings);
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, sourceIndexCompressedXContent, MapperService.MergeReason.INDEX_TEMPLATE);

        assertThat(downsampleIndexMappings.get("_meta"), equalTo(sourceIndexMappings.get("_meta")));

        // Collect expected mappings for fields and dimensions
        Map<String, TimeSeriesParams.MetricType> metricFields = new HashMap<>();
        Map<String, String> dimensionFields = new HashMap<>();
        MappingVisitor.visitMapping(sourceIndexMappings, (field, fieldMapping) -> {
            if (isTimeSeriesMetric(fieldMapping)) {
                metricFields.put(field, TimeSeriesParams.MetricType.fromString(fieldMapping.get(TIME_SERIES_METRIC_PARAM).toString()));
            } else if (hasTimeSeriesDimensionTrue(fieldMapping)) {
                // This includes passthrough objects
                dimensionFields.put(field, fieldMapping.get("type").toString());
            }
        });

        AtomicBoolean encounteredTimestamp = new AtomicBoolean(false);
        Set<String> encounteredMetrics = new HashSet<>();
        Set<String> encounteredDimensions = new HashSet<>();
        MappingVisitor.visitMapping(downsampleIndexMappings, (field, fieldMapping) -> {
            if (field.equals(config.getTimestampField())) {
                encounteredTimestamp.set(true);
                assertThat(fieldMapping.get("type"), equalTo(DateFieldMapper.CONTENT_TYPE));
                Map<String, Object> dateTimeMeta = (Map<String, Object>) fieldMapping.get("meta");
                assertThat(dateTimeMeta.get("time_zone"), equalTo(config.getTimeZone()));
                assertThat(dateTimeMeta.get(config.getIntervalType()), equalTo(config.getInterval().toString()));
            } else if (metricFields.containsKey(field)) {
                encounteredMetrics.add(field);
                TimeSeriesParams.MetricType metricType = metricFields.get(field);
                switch (metricType) {
                    case COUNTER -> assertThat(fieldMapping.get("type"), equalTo("double"));
                    case GAUGE -> {
                        if (config.getSamplingMethod() == DownsampleConfig.SamplingMethod.LAST_VALUE) {
                            assertThat(fieldMapping.get("type"), equalTo("double"));
                        } else {
                            assertThat(fieldMapping.get("type"), equalTo("aggregate_metric_double"));
                        }
                    }
                    case HISTOGRAM -> assertThat(
                        fieldMapping.get("type"),
                        anyOf(equalTo("exponential_histogram"), equalTo("histogram"), equalTo("tdigest"))
                    );
                    default -> fail("Unsupported field type");
                }
                assertThat(fieldMapping.get("time_series_metric"), equalTo(metricType.toString()));
            } else if (dimensionFields.containsKey(field)) {
                encounteredDimensions.add(field);
                assertThat(fieldMapping.get("type"), equalTo(dimensionFields.get(field)));
                assertThat(fieldMapping.get("time_series_dimension"), equalTo(true));
            }
        });
        assertThat(encounteredTimestamp.get(), equalTo(true));
        assertThat(encounteredMetrics, equalTo(metricFields.keySet()));
        assertThat(encounteredDimensions, equalTo(dimensionFields.keySet()));
    }

    private static MapperService getMapperServiceForIndex(String sourceIndex) throws IOException {
        final IndexMetadata indexMetadata = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .getMetadata()
            .getProject(Metadata.DEFAULT_PROJECT_ID)
            .index(sourceIndex);
        final IndicesService indicesService = internalCluster().getAnyMasterNodeInstance(IndicesService.class);
        return indicesService.createIndexMapperServiceForValidation(indexMetadata);
    }

    boolean isTimeSeriesMetric(final Map<String, ?> fieldMapping) {
        final String metricType = (String) fieldMapping.get(TIME_SERIES_METRIC_PARAM);
        return metricType != null
            && List.of(TimeSeriesParams.MetricType.values()).contains(TimeSeriesParams.MetricType.fromString(metricType));
    }

    private static boolean hasTimeSeriesDimensionTrue(Map<String, ?> fieldMapping) {
        return Boolean.TRUE.equals(fieldMapping.get(TIME_SERIES_DIMENSION_PARAM));
    }

    /**
     * How the documents of {@link #createReadOnlyTimeSeriesIndex} are spread over time series and downsampling buckets.
     */
    enum TimeSeriesLayout {
        /** A few time series, with many documents in each bucket. The downsample index is small. */
        FEW_SERIES,
        /**
         * A time series per document, so every document ends up in a bucket of its own. The downsample index is as large as the
         * source index, so a shard-level task sends several bulk requests whose time series do not overlap.
         */
        ONE_SERIES_PER_DOCUMENT,
        /**
         * A single time series with a document per hour, so every document ends up in a bucket of its own when downsampling to
         * {@code 1h}. A shard-level task sends several bulk requests, all for the same time series.
         */
        ONE_SERIES_ONE_DOCUMENT_PER_HOUR
    }

    /**
     * Creates a time series index with a counter and a gauge metric, indexes {@code docCount} documents into a few time series and
     * makes the index read-only, ready to be downsampled.
     */
    void createReadOnlyTimeSeriesIndex(String index, int shards, int replicas, int docCount) throws IOException {
        createReadOnlyTimeSeriesIndex(index, shards, replicas, docCount, TimeSeriesLayout.FEW_SERIES);
    }

    /**
     * Creates a time series index with a counter and a gauge metric, indexes {@code docCount} documents laid out as {@code layout}
     * describes and makes the index read-only, ready to be downsampled.
     */
    void createReadOnlyTimeSeriesIndex(String index, int shards, int replicas, int docCount, TimeSeriesLayout layout) throws IOException {
        logger.info("-> creating time series index [{}] with [{}] shards and [{}] replicas", index, shards, replicas);
        final Settings settings = indexSettings(shards, replicas).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_KEYWORD))
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "1990-09-01T00:00:00Z")
            // Wide enough for a document per hour
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "1994-01-01T00:00:00Z")
            .build();
        final XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        mapping.startObject(FIELD_TIMESTAMP).field("type", "date").endObject();
        mapping.startObject(FIELD_DIMENSION_KEYWORD).field("type", "keyword").field("time_series_dimension", true).endObject();
        mapping.startObject(FIELD_DIMENSION_LONG).field("type", "long").field("time_series_dimension", true).endObject();
        mapping.startObject(FIELD_METRIC_COUNTER_DOUBLE).field("type", "double").field("time_series_metric", "counter").endObject();
        mapping.startObject(FIELD_METRIC_GAUGE_DOUBLE).field("type", "double").field("time_series_metric", "gauge").endObject();
        mapping.endObject().endObject();
        assertAcked(indicesAdmin().prepareCreate(index).setSettings(settings).setMapping(mapping));

        final List<String> dimensionValues = randomList(1, 5, () -> randomAlphaOfLength(6));
        final String singleSeries = randomAlphaOfLength(6);
        final AtomicInteger series = new AtomicInteger();
        final AtomicLong timestamp = new AtomicLong(
            LocalDateTime.parse("1990-09-09T18:00:00").atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()
        );
        final Supplier<XContentBuilder> sourceSupplier = () -> {
            final long ts = switch (layout) {
                case FEW_SERIES, ONE_SERIES_PER_DOCUMENT -> timestamp.getAndAdd(randomIntBetween(0, 30) * 1000L);
                case ONE_SERIES_ONE_DOCUMENT_PER_HOUR -> timestamp.getAndAdd(TimeValue.timeValueHours(1).millis());
            };
            final String dimension = switch (layout) {
                case FEW_SERIES -> randomFrom(dimensionValues);
                case ONE_SERIES_PER_DOCUMENT -> "series-" + series.getAndIncrement();
                case ONE_SERIES_ONE_DOCUMENT_PER_HOUR -> singleSeries;
            };
            try {
                return XContentFactory.jsonBuilder()
                    .startObject()
                    .field(FIELD_TIMESTAMP, DATE_FORMATTER.formatMillis(ts))
                    .field(FIELD_DIMENSION_KEYWORD, dimension)
                    .field(FIELD_DIMENSION_LONG, layout == TimeSeriesLayout.FEW_SERIES ? randomIntBetween(1, 10) : 1)
                    .field(FIELD_METRIC_COUNTER_DOUBLE, (double) ts)
                    .field(FIELD_METRIC_GAUGE_DOUBLE, randomDouble())
                    .endObject();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };
        bulkIndex(index, sourceSupplier, docCount);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true), index);
        ensureGreen(index);
    }

    /**
     * Waits until {@link DownsampleDisruptionPlugin} holds {@code expected} parked executions on nodes that are still running.
     */
    static void awaitParked(int expected) throws Exception {
        assertBusy(() -> assertThat(DownsampleDisruptionPlugin.parkedOnNodes(), hasSize(expected)), 60, TimeUnit.SECONDS);
    }

    /**
     * A document of a downsample index: its version tells whether it was indexed more than once.
     */
    record DocSnapshot(long version, Map<String, Object> source) {}

    /**
     * Asserts that {@code actualIndex} holds the same documents as {@code expectedIndex}, compared by {@code _id} and source.
     * Disruption tests use it to compare a downsample index produced under disruption with one produced without disruption.
     *
     * @return every document of {@code actualIndex} keyed by {@code _id}
     */
    static Map<String, DocSnapshot> assertSameDocuments(String expectedIndex, String actualIndex) {
        final Map<String, DocSnapshot> expected = fetchAllDocs(expectedIndex);
        final Map<String, DocSnapshot> actual = fetchAllDocs(actualIndex);
        assertThat(actual.keySet(), equalTo(expected.keySet()));
        for (var entry : expected.entrySet()) {
            assertThat("source of [" + entry.getKey() + "]", actual.get(entry.getKey()).source(), equalTo(entry.getValue().source()));
        }
        return actual;
    }

    /**
     * Asserts that no document was indexed more than once, which would have bumped its version. A shard-level task that runs
     * again after an interruption recomputes buckets the interrupted run already indexed, they must be skipped, not rewritten.
     */
    static void assertNoDocumentRewritten(Map<String, DocSnapshot> docs) {
        final List<String> rewritten = docs.entrySet()
            .stream()
            .filter(entry -> entry.getValue().version() > 1)
            .map(Map.Entry::getKey)
            .sorted()
            .toList();
        assertThat(
            "[" + rewritten.size() + "] documents indexed more than once, e.g. " + rewritten.stream().limit(5).toList(),
            rewritten,
            empty()
        );
    }

    /**
     * @return the number of documents of {@code index} that were rejected because a document with the same {@code _id} existed
     */
    static long versionConflicts(String index) {
        return indicesAdmin().prepareStats(index).get().getPrimaries().getIndexing().getTotal().getIndexFailedDueToVersionConflictCount();
    }

    /**
     * @return the number of searchable documents of {@code index}, after refreshing it
     */
    static long countDocuments(String index) {
        indicesAdmin().prepareRefresh(index).get();
        return SearchResponseUtils.getTotalHitsValue(prepareSearch(index).setSize(0).setTrackTotalHits(true));
    }

    private static Map<String, DocSnapshot> fetchAllDocs(String index) {
        final int maxDocs = 100_000;
        final long count = countDocuments(index);
        assertThat(count, lessThan((long) maxDocs));
        if (count > IndexSettings.MAX_RESULT_WINDOW_SETTING.getDefault(Settings.EMPTY)) {
            updateIndexSettings(Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), maxDocs), index);
        }
        final var response = prepareSearch(index).setSize((int) count).setVersion(true).get();
        try {
            final Map<String, DocSnapshot> docs = new HashMap<>();
            for (SearchHit hit : response.getHits().getHits()) {
                docs.put(hit.getId(), new DocSnapshot(hit.getVersion(), hit.getSourceAsMap()));
            }
            assertThat(docs.size(), equalTo((int) count));
            return docs;
        } finally {
            response.decRef();
        }
    }

    public static DownsampleConfig.SamplingMethod randomSamplingMethod() {
        if (between(0, DownsampleConfig.SamplingMethod.values().length) == 0) {
            return null;
        } else {
            return randomFrom(DownsampleConfig.SamplingMethod.values());
        }
    }
}
