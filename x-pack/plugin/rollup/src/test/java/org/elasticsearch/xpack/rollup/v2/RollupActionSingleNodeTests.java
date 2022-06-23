/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;
import org.elasticsearch.xpack.core.rollup.action.RollupActionRequestValidationException;
import org.elasticsearch.xpack.rollup.Rollup;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;

public class RollupActionSingleNodeTests extends ESSingleNodeTestCase {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    public static final String FIELD_TIMESTAMP = "@timestamp";
    public static final String FIELD_DIMENSION_1 = "dimension_kw";
    public static final String FIELD_DIMENSION_2 = "dimension_long";
    public static final String FIELD_NUMERIC_1 = "numeric_1";
    public static final String FIELD_NUMERIC_2 = "numeric_2";
    public static final String FIELD_METRIC_LABEL_DOUBLE = "metric_label_double";
    public static final String FIELD_LABEL_DOUBLE = "label_double";
    public static final String FIELD_LABEL_INTEGER = "label_integer";
    public static final String FIELD_LABEL_KEYWORD = "label_keyword";
    public static final String FIELD_LABEL_TEXT = "label_text";
    public static final String FIELD_LABEL_BOOLEAN = "label_boolean";
    public static final String FIELD_LABEL_IPv4_ADDRESS = "label_ipv4_address";
    public static final String FIELD_LABEL_IPv6_ADDRESS = "label_ipv6_address";
    public static final String FIELD_LABEL_DATE = "label_date";

    private static final int MAX_DIM_VALUES = 5;
    private static final long MAX_NUM_BUCKETS = 10;

    private String sourceIndex, rollupIndex;
    private long startTime;
    private int docCount, numOfShards, numOfReplicas;
    private List<String> dimensionValues;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(
            LocalStateCompositeXPackPlugin.class,
            Rollup.class,
            AnalyticsPlugin.class,
            AggregateMetricMapperPlugin.class,
            DataStreamsPlugin.class
        );
    }

    @Before
    public void setup() {
        sourceIndex = getTestName().toLowerCase(Locale.ROOT) + "-" + randomAlphaOfLength(4).toLowerCase(Locale.ROOT);
        rollupIndex = "rollup-" + sourceIndex;
        startTime = randomLongBetween(946769284000L, 1607470084000L); // random date between 2000-2020
        docCount = randomIntBetween(10, 9000);
        numOfShards = randomIntBetween(1, 4);
        numOfReplicas = 0; // Since this is a single node, we cannot have replicas

        // Values for keyword dimensions
        dimensionValues = new ArrayList<>(MAX_DIM_VALUES);
        for (int j = 0; j < randomIntBetween(1, MAX_DIM_VALUES); j++) {
            dimensionValues.add(randomAlphaOfLength(6));
        }

        /**
         * NOTE: here we map each numeric label field also as a (counter) metric.
         * This is done for testing purposes. There is no easy way to test
         * that labels are collected using the last value. The idea is to
         * check that the value of the label (last value) matches the value
         * of the corresponding metric which uses a last_value metric type.
         */
        client().admin()
            .indices()
            .prepareCreate(sourceIndex)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", numOfShards)
                    .put("index.number_of_replicas", numOfReplicas)
                    .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                    .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1))
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(startTime).toString())
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z")
                    .build()
            )
            .setMapping(
                FIELD_TIMESTAMP,
                "type=date",
                FIELD_DIMENSION_1,
                "type=keyword,time_series_dimension=true",
                FIELD_DIMENSION_2,
                "type=long,time_series_dimension=true",
                FIELD_NUMERIC_1,
                "type=long,time_series_metric=gauge",
                FIELD_NUMERIC_2,
                "type=double,time_series_metric=counter",
                FIELD_LABEL_DOUBLE,
                "type=double",
                FIELD_LABEL_INTEGER,
                "type=integer",
                FIELD_LABEL_KEYWORD,
                "type=keyword",
                FIELD_LABEL_TEXT,
                "type=text",
                // FIELD_LABEL_BOOLEAN,
                // "type=boolean",
                FIELD_METRIC_LABEL_DOUBLE, /* numeric label indexed as a metric */
                "type=double,time_series_metric=counter",
                FIELD_LABEL_IPv4_ADDRESS,
                "type=ip",
                FIELD_LABEL_IPv6_ADDRESS,
                "type=ip",
                FIELD_LABEL_DATE,
                "type=date"
            )
            .get();
    }

    public void testRollupIndex() throws IOException {
        RollupActionConfig config = new RollupActionConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> {
            String ts = randomDateForInterval(config.getInterval());
            double labelDoubleValue = DATE_FORMATTER.parseMillis(ts);
            int labelIntegerValue = randomInt();
            String labelIpv4Address = NetworkAddress.format(randomIp(true));
            String labelIpv6Address = NetworkAddress.format(randomIp(false));
            Date labelDateValue = randomDate();
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                // .field(FIELD_DIMENSION_2, randomIntBetween(1, 10)) //TODO: Fix _tsid format issue and then enable this
                .field(FIELD_NUMERIC_1, randomInt())
                .field(FIELD_NUMERIC_2, DATE_FORMATTER.parseMillis(ts))
                .field(FIELD_LABEL_DOUBLE, labelDoubleValue)
                .field(FIELD_METRIC_LABEL_DOUBLE, labelDoubleValue)
                .field(FIELD_LABEL_INTEGER, labelIntegerValue)
                .field(FIELD_LABEL_KEYWORD, ts)
                .field(FIELD_LABEL_TEXT, ts)
                // .field(FIELD_LABEL_BOOLEAN, randomBoolean())
                .field(FIELD_LABEL_IPv4_ADDRESS, labelIpv4Address)
                .field(FIELD_LABEL_IPv6_ADDRESS, labelIpv6Address)
                .field(FIELD_LABEL_DATE, labelDateValue)
                .endObject();
        };
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex);
        // Clone the source index before rollup deletes it
        String sourceIndexClone = cloneSourceIndex(sourceIndex);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(config, sourceIndex, sourceIndexClone, rollupIndex);
    }

    private Date randomDate() {
        int randomYear = randomIntBetween(1970, 2020);
        int randomMonth = randomIntBetween(1, 12);
        int randomDayOfMonth = randomIntBetween(1, 28);
        int randomHour = randomIntBetween(0, 23);
        int randomMinute = randomIntBetween(0, 59);
        int randomSecond = randomIntBetween(0, 59);
        return Date.from(
            ZonedDateTime.of(randomYear, randomMonth, randomDayOfMonth, randomHour, randomMinute, randomSecond, 0, ZoneOffset.UTC)
                .toInstant()
        );
    }

    public void testNullSourceIndexName() {
        RollupActionConfig config = new RollupActionConfig(randomInterval());
        ActionRequestValidationException exception = expectThrows(
            ActionRequestValidationException.class,
            () -> rollup(null, rollupIndex, config)
        );
        assertThat(exception.getMessage(), containsString("source index is missing"));
    }

    public void testNullRollupIndexName() {
        RollupActionConfig config = new RollupActionConfig(randomInterval());
        ActionRequestValidationException exception = expectThrows(
            ActionRequestValidationException.class,
            () -> rollup(sourceIndex, null, config)
        );
        assertThat(exception.getMessage(), containsString("rollup index name is missing"));
    }

    public void testNullRollupConfig() {
        ActionRequestValidationException exception = expectThrows(
            ActionRequestValidationException.class,
            () -> rollup(sourceIndex, rollupIndex, null)
        );
        assertThat(exception.getMessage(), containsString("rollup configuration is missing"));
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "TODO: Fix this")
    public void testRollupSparseMetrics() throws IOException {
        RollupActionConfig config = new RollupActionConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues));
            if (randomBoolean()) {
                builder.field(FIELD_NUMERIC_1, randomInt());
            }
            if (randomBoolean()) {
                builder.field(FIELD_NUMERIC_2, randomDouble());
            }
            return builder.endObject();
        };
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex);
        // Clone the source index before rollup deletes it
        String sourceIndexClone = cloneSourceIndex(sourceIndex);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(config, sourceIndex, sourceIndexClone, rollupIndex);
    }

    public void testCannotRollupToExistingIndex() throws Exception {
        RollupActionConfig config = new RollupActionConfig(randomInterval());
        prepareSourceIndex(sourceIndex);

        // Create an empty index with the same name as the rollup index
        client().admin().indices().prepareCreate(rollupIndex).get();
        ResourceAlreadyExistsException exception = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> rollup(sourceIndex, rollupIndex, config)
        );
        assertThat(exception.getMessage(), containsString(rollupIndex));
    }

    public void testRollupEmptyIndex() {
        RollupActionConfig config = new RollupActionConfig(randomInterval());
        // Source index has been created in the setup() method
        prepareSourceIndex(sourceIndex);
        // Clone the source index before rollup deletes it
        String sourceIndexClone = cloneSourceIndex(sourceIndex);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(config, sourceIndex, sourceIndexClone, rollupIndex);
    }

    public void testCannotRollupIndexWithNoMetrics() {
        // Create a source index that contains no metric fields in its mapping
        String sourceIndex = "no-metrics-idx-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        client().admin()
            .indices()
            .prepareCreate(sourceIndex)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", numOfShards)
                    .put("index.number_of_replicas", numOfReplicas)
                    .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                    .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1))
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(startTime).toString())
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z")
                    .build()
            )
            .setMapping(
                FIELD_TIMESTAMP,
                "type=date",
                FIELD_DIMENSION_1,
                "type=keyword,time_series_dimension=true",
                FIELD_DIMENSION_2,
                "type=long,time_series_dimension=true"
            )
            .get();

        RollupActionConfig config = new RollupActionConfig(randomInterval());
        prepareSourceIndex(sourceIndex);
        Exception exception = expectThrows(RollupActionRequestValidationException.class, () -> rollup(sourceIndex, rollupIndex, config));
        assertThat(exception.getMessage(), containsString("does not contain any metric fields"));
    }

    public void testCannotRollupWriteableIndex() {
        RollupActionConfig config = new RollupActionConfig(randomInterval());
        // Source index has been created in the setup() method and is empty and still writable
        Exception exception = expectThrows(ElasticsearchException.class, () -> rollup(sourceIndex, rollupIndex, config));
        assertThat(exception.getMessage(), containsString("Rollup requires setting [index.blocks.write = true] for index"));
    }

    public void testCannotRollupMissingIndex() {
        RollupActionConfig config = new RollupActionConfig(randomInterval());
        IndexNotFoundException exception = expectThrows(IndexNotFoundException.class, () -> rollup("missing-index", rollupIndex, config));
        assertEquals("missing-index", exception.getIndex().getName());
        assertThat(exception.getMessage(), containsString("no such index [missing-index]"));
    }

    public void testCannotRollupWhileOtherRollupInProgress() throws Exception {
        RollupActionConfig config = new RollupActionConfig(randomInterval());
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_DIMENSION_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        prepareSourceIndex(sourceIndex);
        client().execute(RollupAction.INSTANCE, new RollupAction.Request(sourceIndex, rollupIndex, config), ActionListener.noop());
        ResourceAlreadyExistsException exception = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> rollup(sourceIndex, rollupIndex, config)
        );
        assertThat(exception.getMessage(), containsString(rollupIndex));
    }

    public void testRollupDatastream() throws Exception {
        RollupActionConfig config = new RollupActionConfig(randomInterval());
        String dataStreamName = createDataStream();

        final Instant now = Instant.now();
        SourceSupplier sourceSupplier = () -> {
            String ts = randomDateForRange(now.minusSeconds(60 * 60).toEpochMilli(), now.plusSeconds(60 * 60).toEpochMilli());
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                .field(FIELD_NUMERIC_1, randomInt())
                .field(FIELD_NUMERIC_2, DATE_FORMATTER.parseMillis(ts))
                .endObject();
        };
        bulkIndex(dataStreamName, sourceSupplier);

        String sourceIndex = rollover(dataStreamName).getOldIndex();
        prepareSourceIndex(sourceIndex);
        // Clone the source index before rollup deletes it
        String sourceIndexClone = cloneSourceIndex(sourceIndex);
        String rollupIndex = "rollup-" + sourceIndex;
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(config, sourceIndex, sourceIndexClone, rollupIndex);

        var r = client().execute(GetDataStreamAction.INSTANCE, new GetDataStreamAction.Request(new String[] { dataStreamName })).get();
        assertEquals(1, r.getDataStreams().size());
        List<Index> indices = r.getDataStreams().get(0).getDataStream().getIndices();
        // Assert that the rollup index is a member of the data stream
        assertFalse(indices.stream().filter(i -> i.getName().equals(rollupIndex)).toList().isEmpty());
        // Assert that the source index is not a member of the data stream
        assertTrue(indices.stream().filter(i -> i.getName().equals(sourceIndex)).toList().isEmpty());
    }

    private DateHistogramInterval randomInterval() {
        return ConfigTestHelpers.randomInterval();
    }

    private String randomDateForInterval(DateHistogramInterval interval) {
        long endTime = startTime + MAX_NUM_BUCKETS * interval.estimateMillis();
        return randomDateForRange(startTime, endTime);
    }

    private String randomDateForRange(long start, long end) {
        return DATE_FORMATTER.formatMillis(randomLongBetween(start, end));
    }

    /**
     * The source index is deleted at the end of the rollup process.
     * We clone the source index, so that we validate rollup results against the
     * source index clone.
     *
     * @return the name of the source index clone
     */
    private String cloneSourceIndex(String sourceIndex) {
        String sourceIndexClone = "clone-" + sourceIndex;
        ResizeResponse r = client().admin()
            .indices()
            .prepareResizeIndex(sourceIndex, sourceIndexClone)
            .setResizeType(ResizeType.CLONE)
            .setSettings(
                Settings.builder().put("index.number_of_shards", numOfShards).put("index.number_of_replicas", numOfReplicas).build()
            )
            .get();
        assertTrue(r.isAcknowledged());
        return sourceIndexClone;
    }

    private void bulkIndex(SourceSupplier sourceSupplier) throws IOException {
        bulkIndex(sourceIndex, sourceSupplier);
    }

    private void bulkIndex(String indexName, SourceSupplier sourceSupplier) throws IOException {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docCount; i++) {
            IndexRequest indexRequest = new IndexRequest(indexName).opType(DocWriteRequest.OpType.CREATE);
            XContentBuilder source = sourceSupplier.get();
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
                    logger.debug("We tried to insert a duplicate: [{}]", response.getFailureMessage());
                    duplicates++;
                } else {
                    fail("Failed to index data: " + bulkResponse.buildFailureMessage());
                }
            }
        }
        int docsIndexed = docCount - duplicates;
        logger.info("Indexed [{}] documents. Dropped [{}] duplicates.", docsIndexed, duplicates);
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), docsIndexed);
    }

    private void prepareSourceIndex(String sourceIndex) {
        // Set the source index to read-only state
        AcknowledgedResponse r = client().admin()
            .indices()
            .prepareUpdateSettings(sourceIndex)
            .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build())
            .get();
        assertTrue(r.isAcknowledged());
    }

    private void rollup(String sourceIndex, String rollupIndex, RollupActionConfig config) {
        AcknowledgedResponse response = client().execute(RollupAction.INSTANCE, new RollupAction.Request(sourceIndex, rollupIndex, config))
            .actionGet();
        assertTrue(response.isAcknowledged());
    }

    private RolloverResponse rollover(String dataStreamName) throws ExecutionException, InterruptedException {
        RolloverResponse response = client().admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).get();
        assertTrue(response.isAcknowledged());
        return response;
    }

    private Aggregations aggregate(final String index, AggregationBuilder aggregationBuilder) {
        return client().prepareSearch(index).addAggregation(aggregationBuilder).get().getAggregations();
    }

    @SuppressWarnings("unchecked")
    private void assertRollupIndex(RollupActionConfig config, String sourceIndex, String sourceIndexClone, String rollupIndex) {
        // Retrieve field information for the metric fields
        FieldCapabilitiesResponse fieldCapsResponse = client().prepareFieldCaps(sourceIndexClone).setFields("*").get();
        Map<String, TimeSeriesParams.MetricType> metricFields = getMetricFields(fieldCapsResponse);
        Map<String, String> labelFields = getLabelFields(fieldCapsResponse);

        final AggregationBuilder aggregations = buildAggregations(config, metricFields, labelFields, config.getTimestampField());
        Aggregations origResp = aggregate(sourceIndexClone, aggregations);
        Aggregations rollupResp = aggregate(rollupIndex, aggregations);
        assertEquals(origResp.asMap().keySet(), rollupResp.asMap().keySet());

        StringTerms originalTsIdTermsAggregation = (StringTerms) origResp.getAsMap().values().stream().toList().get(0);
        StringTerms rollupTsIdTermsAggregation = (StringTerms) rollupResp.getAsMap().values().stream().toList().get(0);
        originalTsIdTermsAggregation.getBuckets().forEach(originalBucket -> {

            StringTerms.Bucket rollupBucket = rollupTsIdTermsAggregation.getBucketByKey(originalBucket.getKeyAsString());
            assertEquals(originalBucket.getAggregations().asList().size(), rollupBucket.getAggregations().asList().size());

            InternalDateHistogram originalDateHistogram = (InternalDateHistogram) originalBucket.getAggregations().asList().get(0);
            InternalDateHistogram rollupDateHistogram = (InternalDateHistogram) rollupBucket.getAggregations().asList().get(0);
            List<InternalDateHistogram.Bucket> originalDateHistogramBuckets = originalDateHistogram.getBuckets();
            List<InternalDateHistogram.Bucket> rollupDateHistogramBuckets = rollupDateHistogram.getBuckets();
            assertEquals(originalDateHistogramBuckets.size(), rollupDateHistogramBuckets.size());

            for (int i = 0; i < originalDateHistogramBuckets.size(); ++i) {
                InternalDateHistogram.Bucket originalDateHistogramBucket = originalDateHistogramBuckets.get(i);
                InternalDateHistogram.Bucket rollupDateHistogramBucket = rollupDateHistogramBuckets.get(i);
                assertEquals(originalDateHistogramBucket.getKeyAsString(), rollupDateHistogramBucket.getKeyAsString());

                Aggregations originalAggregations = originalDateHistogramBucket.getAggregations();
                Aggregations rollupAggregations = rollupDateHistogramBucket.getAggregations();
                assertEquals(originalAggregations.asList().size(), rollupAggregations.asList().size());

                List<Aggregation> nonTopHitsOriginalAggregations = originalAggregations.asList()
                    .stream()
                    .filter(agg -> agg.getType().equals("top_hits") == false)
                    .toList();
                List<Aggregation> nonTopHitsRollupAggregations = rollupAggregations.asList()
                    .stream()
                    .filter(agg -> agg.getType().equals("top_hits") == false)
                    .toList();
                assertEquals(nonTopHitsOriginalAggregations, nonTopHitsRollupAggregations);

                List<Aggregation> topHitsOriginalAggregations = originalAggregations.asList()
                    .stream()
                    .filter(agg -> agg.getType().equals("top_hits"))
                    .toList();
                List<Aggregation> topHitsRollupAggregations = rollupAggregations.asList()
                    .stream()
                    .filter(agg -> agg.getType().equals("top_hits"))
                    .toList();
                assertEquals(topHitsRollupAggregations.size(), topHitsRollupAggregations.size());

                for (int j = 0; j < topHitsRollupAggregations.size(); ++j) {
                    InternalTopHits originalTopHits = (InternalTopHits) topHitsOriginalAggregations.get(j);
                    InternalTopHits rollupTopHits = (InternalTopHits) topHitsRollupAggregations.get(j);
                    SearchHit[] originalHits = originalTopHits.getHits().getHits();
                    SearchHit[] rollupHits = rollupTopHits.getHits().getHits();
                    assertEquals(originalHits.length, rollupHits.length);

                    for (int k = 0; k < originalHits.length; ++k) {
                        SearchHit originalHit = originalHits[k];
                        SearchHit rollupHit = rollupHits[k];

                        Map<String, DocumentField> originalHitDocumentFields = originalHit.getDocumentFields();
                        Map<String, DocumentField> rollupHitDocumentFields = rollupHit.getDocumentFields();
                        assertEquals(originalHitDocumentFields, rollupHitDocumentFields);

                        // NOTE: here we take advantage of the fact that a label field is indexed also as a metric of type
                        // `counter`. This way we can actually check that the label value stored in the rollup index
                        // is the last value (which is what we store for a metric of type counter) by comparing the metric
                        // field value to the label field value.
                        Object originalLabelValue = originalHit.getDocumentFields().values().stream().toList().get(0).getValue();
                        Object rollupLabelValue = rollupHit.getDocumentFields().values().stream().toList().get(0).getValue();
                        Optional<Aggregation> labelAsMetric = nonTopHitsOriginalAggregations.stream()
                            .filter(agg -> agg.getName().equals("metric_" + rollupTopHits.getName()))
                            .findFirst();
                        // NOTE: this check is possible only if the label can be indexed as a metric (the label is a numeric field)
                        if (labelAsMetric.isPresent()) {
                            double metricValue = ((Max) labelAsMetric.get()).value();
                            assertEquals(metricValue, rollupLabelValue);
                        }
                        assertEquals(originalLabelValue, rollupLabelValue);
                    }
                }
            }
        });

        GetIndexResponse indexSettingsResp = client().admin().indices().prepareGetIndex().addIndices(sourceIndexClone, rollupIndex).get();
        // Assert rollup metadata are set in index settings
        assertEquals("success", indexSettingsResp.getSetting(rollupIndex, "index.rollup.status"));
        assertEquals(
            indexSettingsResp.getSetting(sourceIndexClone, "index.resize.source.uuid"),
            indexSettingsResp.getSetting(rollupIndex, "index.rollup.source.uuid")
        );
        assertEquals(
            indexSettingsResp.getSetting(sourceIndexClone, "index.resize.source.name"),
            indexSettingsResp.getSetting(rollupIndex, "index.rollup.source.name")
        );
        assertEquals(indexSettingsResp.getSetting(sourceIndexClone, "index.mode"), indexSettingsResp.getSetting(rollupIndex, "index.mode"));
        assertEquals(
            indexSettingsResp.getSetting(sourceIndexClone, "time_series.start_time"),
            indexSettingsResp.getSetting(rollupIndex, "time_series.start_time")
        );
        assertEquals(
            indexSettingsResp.getSetting(sourceIndexClone, "time_series.end_time"),
            indexSettingsResp.getSetting(rollupIndex, "time_series.end_time")
        );
        assertEquals(
            indexSettingsResp.getSetting(sourceIndexClone, "index.routing_path"),
            indexSettingsResp.getSetting(rollupIndex, "index.routing_path")
        );
        assertEquals(
            indexSettingsResp.getSetting(sourceIndexClone, "index.number_of_shards"),
            indexSettingsResp.getSetting(rollupIndex, "index.number_of_shards")
        );
        assertEquals(
            indexSettingsResp.getSetting(sourceIndexClone, "index.number_of_replicas"),
            indexSettingsResp.getSetting(rollupIndex, "index.number_of_replicas")
        );
        assertEquals("true", indexSettingsResp.getSetting(rollupIndex, "index.blocks.write"));

        // Assert field mappings
        Map<String, Map<String, Object>> mappings = (Map<String, Map<String, Object>>) indexSettingsResp.getMappings()
            .get(rollupIndex)
            .getSourceAsMap()
            .get("properties");

        assertEquals(DateFieldMapper.CONTENT_TYPE, mappings.get(config.getTimestampField()).get("type"));
        Map<String, Object> dateTimeMeta = (Map<String, Object>) mappings.get(config.getTimestampField()).get("meta");
        assertEquals(config.getTimeZone(), dateTimeMeta.get("time_zone"));
        assertEquals(config.getInterval().toString(), dateTimeMeta.get(config.getIntervalType()));

        metricFields.forEach((field, metricType) -> {
            switch (metricType) {
                case counter -> assertEquals("double", mappings.get(field).get("type"));
                case gauge -> assertEquals("aggregate_metric_double", mappings.get(field).get("type"));
                default -> fail("Unsupported field type");
            }
            assertEquals(metricType.toString(), mappings.get(field).get("time_series_metric"));
        });

        // Assert that source index was removed
        expectThrows(IndexNotFoundException.class, () -> client().admin().indices().prepareGetIndex().addIndices(sourceIndex).get());
    }

    private Map<String, String> getLabelFields(FieldCapabilitiesResponse fieldCapsResponse) {
        return fieldCapsResponse.get().entrySet().stream().filter(e -> {
            FieldCapabilities fieldCapabilities = e.getValue().values().iterator().next();
            return fieldCapabilities.getMetricType() == null
                && fieldCapabilities.isLabel()
                && fieldCapabilities.getName().equals("@timestamp") == false;
        }).collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().values().iterator().next().getType()));
    }

    private Map<String, TimeSeriesParams.MetricType> getMetricFields(FieldCapabilitiesResponse fieldCapsResponse) {
        return fieldCapsResponse.get()
            .entrySet()
            .stream()
            .filter(e -> e.getValue().values().iterator().next().getMetricType() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().values().iterator().next().getMetricType()));
    }

    private AggregationBuilder buildAggregations(
        final RollupActionConfig config,
        final Map<String, TimeSeriesParams.MetricType> metrics,
        final Map<String, String> labels,
        final String timestampField
    ) {

        final TermsAggregationBuilder tsidAggregation = new TermsAggregationBuilder("tsid").field(TimeSeriesIdFieldMapper.NAME)
            .size(10_000);
        final DateHistogramAggregationBuilder dateHistogramAggregation = new DateHistogramAggregationBuilder("timestamp").field(
            config.getTimestampField()
        ).fixedInterval(config.getInterval());
        if (config.getTimeZone() != null) {
            dateHistogramAggregation.timeZone(ZoneId.of(config.getTimeZone()));
        }
        tsidAggregation.subAggregation(dateHistogramAggregation);

        metrics.forEach((fieldName, metricType) -> {
            for (final String supportedAggregation : metricType.supportedAggs()) {
                switch (supportedAggregation) {
                    case "min" -> dateHistogramAggregation.subAggregation(
                        new MinAggregationBuilder(fieldName + "_" + supportedAggregation).field(fieldName)
                    );
                    case "max", "last_value" -> dateHistogramAggregation.subAggregation(
                        new MaxAggregationBuilder(fieldName + "_" + supportedAggregation).field(fieldName)
                    );
                    case "sum" -> dateHistogramAggregation.subAggregation(
                        new SumAggregationBuilder(fieldName + "_" + supportedAggregation).field(fieldName)
                    );
                    case "value_count" -> dateHistogramAggregation.subAggregation(
                        new ValueCountAggregationBuilder(fieldName + "_" + supportedAggregation).field(fieldName)
                    );
                    default -> throw new IllegalArgumentException("Unsupported metric type [" + supportedAggregation + "]");
                }
            }
        });

        labels.forEach((fieldName, type) -> {
            dateHistogramAggregation.subAggregation(
                new TopHitsAggregationBuilder(fieldName + "_last_value").sort(SortBuilders.fieldSort(timestampField).order(SortOrder.DESC))
                    .size(1)
                    .fetchField(fieldName)
            );
        });

        return tsidAggregation;
    }

    @FunctionalInterface
    public interface SourceSupplier {
        XContentBuilder get() throws IOException;
    }

    private String createDataStream() throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.getDefault());
        Template indexTemplate = new Template(
            Settings.builder()
                .put("index.number_of_shards", numOfShards)
                .put("index.number_of_replicas", numOfReplicas)
                .put("index.mode", "time_series")
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1))
                .build(),
            new CompressedXContent("""
                {
                    "properties": {
                        "@timestamp" : {
                            "type": "date"
                        },
                        "dimension_kw": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "dimension_long": {
                            "type": "long",
                            "time_series_dimension": true
                        },
                        "numeric_1": {
                            "type": "long",
                            "time_series_metric": "gauge"
                        },
                        "numeric_2": {
                            "type": "double",
                            "time_series_metric": "counter"
                        }
                    }
                }
                """),
            null
        );

        ComposableIndexTemplate template = new ComposableIndexTemplate(
            List.of(dataStreamName + "*"),
            indexTemplate,
            null,
            null,
            null,
            null,
            new ComposableIndexTemplate.DataStreamTemplate(false, false),
            null
        );
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(dataStreamName + "_template")
            .indexTemplate(template);
        AcknowledgedResponse response = client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();

        assertTrue(response.isAcknowledged());
        assertTrue(
            client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)).get().isAcknowledged()
        );
        return dataStreamName;
    }
}
