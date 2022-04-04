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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.elasticsearch.xpack.rollup.Rollup;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

//@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/69799")
public class RollupActionSingleNodeTests extends ESSingleNodeTestCase {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    public static final String FIELD_TIMESTAMP = "@timestamp";
    public static final String FIELD_CATEGORICAL_1 = "categorical_1";
    public static final String FIELD_NUMERIC_1 = "numeric_1";
    public static final String FIELD_NUMERIC_2 = "numeric_2";

    public static final int MAX_DIMS = 4;

    public static final TermsGroupConfig ROLLUP_TERMS_CONFIG = new TermsGroupConfig(FIELD_CATEGORICAL_1);
    public static final long MAX_NUM_BUCKETS = 10;

    private String sourceIndex, rollupIndex;
    private long startTime;
    private int docCount;
    private Map<String, List<String>> dimensions;

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
        sourceIndex = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        rollupIndex = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        startTime = randomLongBetween(946769284000L, 1607470084000L); // random date between 2000-2020
        docCount = 5000; // randomIntBetween(10, 2500);

        // Create dimensions
        dimensions = new HashMap<>(MAX_DIMS);
        for (int i = 0; i < randomIntBetween(1, MAX_DIMS); i++) {
            List<String> l = new ArrayList<>(MAX_DIMS);
            String key = "dim-" + i;
            for (int j = 0; j < randomIntBetween(1, MAX_DIMS); j++) {
                l.add(key + "-" + j);
            }
            dimensions.put(key, l);
        }

        client().admin()
            .indices()
            .prepareCreate(sourceIndex)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", randomIntBetween(1, 4))
                    .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                    .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_CATEGORICAL_1))
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(startTime).toString())
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z")
                    .build()
            )
            .setMapping(
                FIELD_TIMESTAMP,
                "type=date",
                FIELD_CATEGORICAL_1,
                "type=keyword,time_series_dimension=true",
                FIELD_NUMERIC_1,
                "type=double,time_series_metric=gauge",
                FIELD_NUMERIC_2,
                "type=float,time_series_metric=gauge"
            )
            .get();
    }

    public void testCannotRollupToExistingIndex() throws Exception {
        RollupActionConfig config = new RollupActionConfig(randomInterval(), null);
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_CATEGORICAL_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(config, sourceIndex, rollupIndex);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> rollup(sourceIndex, rollupIndex, config));
        assertThat(exception.getMessage(), containsString("Unable to rollup index [" + sourceIndex + "]"));
    }

    public void testTemporaryIndexCannotBeCreatedAlreadyExists() {
        assertTrue(client().admin().indices().prepareCreate(".rolluptmp-" + rollupIndex).get().isAcknowledged());
        RollupActionConfig config = new RollupActionConfig(randomInterval(), null);
        Exception exception = expectThrows(ElasticsearchException.class, () -> rollup(sourceIndex, rollupIndex, config));
        assertThat(exception.getMessage(), containsString("already exists"));
    }

    public void testCannotRollupWhileOtherRollupInProgress() throws Exception {
        RollupActionConfig config = new RollupActionConfig(randomInterval(), null);
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_CATEGORICAL_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        client().execute(RollupAction.INSTANCE, new RollupAction.Request(sourceIndex, rollupIndex, config), ActionListener.noop());
        ResourceAlreadyExistsException exception = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> rollup(sourceIndex, rollupIndex, config)
        );
        assertThat(exception.getMessage(), containsString(".rolluptmp-" + rollupIndex));
    }

    public void testMinMaxMetrics() throws IOException {
        RollupActionConfig config = new RollupActionConfig(randomInterval(), null);
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_CATEGORICAL_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(sourceSupplier);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(config, sourceIndex, rollupIndex);
    }

    public void testSparseMetrics() throws IOException {
        RollupActionConfig config = new RollupActionConfig(randomInterval(), null);
        SourceSupplier sourceSupplier = () -> {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
                .field(FIELD_CATEGORICAL_1, randomAlphaOfLength(1));

            if (randomBoolean()) {
                builder.field(FIELD_NUMERIC_1, randomDouble());
            }
            return builder.endObject();
        };
        bulkIndex(sourceSupplier);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(config, sourceIndex, rollupIndex);
    }

    public void testSumValueCountMetric() throws IOException {
        RollupActionConfig config = new RollupActionConfig(randomInterval(), null);
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_CATEGORICAL_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomInt())
            .field("_doc_count", randomIntBetween(1, 10))
            .endObject();
        bulkIndex(sourceSupplier);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(config, sourceIndex, rollupIndex);
    }

    public void testAvgMetric() throws IOException {
        RollupActionConfig config = new RollupActionConfig(randomInterval(), null);
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_CATEGORICAL_1, randomAlphaOfLength(1))
            // Use integers to ensure that avg is comparable between rollup and original
            .field(FIELD_NUMERIC_1, randomInt())
            .endObject();
        bulkIndex(sourceSupplier);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(config, sourceIndex, rollupIndex);
    }

    public void testAllMetrics() throws IOException {
        RollupActionConfig config = new RollupActionConfig(randomInterval(), null);
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_CATEGORICAL_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomInt())
            .endObject();
        bulkIndex(sourceSupplier);
        rollup(sourceIndex, rollupIndex, config);
        assertRollupIndex(config, sourceIndex, rollupIndex);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "TODO")
    public void testRollupDatastream() throws Exception {
        RollupActionConfig config = new RollupActionConfig(randomInterval(), null);
        String dataStreamName = createDataStream();

        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD_TIMESTAMP, randomDateForInterval(config.getInterval()))
            .field(FIELD_CATEGORICAL_1, randomAlphaOfLength(1))
            .field(FIELD_NUMERIC_1, randomDouble())
            .endObject();
        bulkIndex(dataStreamName, sourceSupplier);

        String oldIndexName = rollover(dataStreamName).getOldIndex();
        String rollupIndexName = ".rollup-" + oldIndexName;
        rollup(oldIndexName, rollupIndexName, config);
        assertRollupIndex(config, oldIndexName, rollupIndexName);
        rollup(oldIndexName, rollupIndexName + "-2", config);
        assertRollupIndex(config, oldIndexName, rollupIndexName + "-2");
    }

    private DateHistogramInterval randomInterval() {
        // return ConfigTestHelpers.randomInterval();
        return DateHistogramInterval.days(30);
    }

    private String randomDateForInterval(DateHistogramInterval interval) {
        long endTime = startTime + MAX_NUM_BUCKETS * interval.estimateMillis();
        return DATE_FORMATTER.formatMillis(randomLongBetween(startTime, endTime));
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
                    logger.info("We tried to insert a duplicate: " + response.getFailureMessage());
                    duplicates++;
                } else {
                    fail("Failed to index data: " + bulkResponse.buildFailureMessage());
                }
            }
        }
        int docsIndexed = docCount - duplicates;
        logger.info("Indexed [" + docsIndexed + "] documents");
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), docsIndexed);
    }

    private void rollup(String sourceIndex, String rollupIndex, RollupActionConfig config) {
        AcknowledgedResponse rollupResponse = client().execute(
            RollupAction.INSTANCE,
            new RollupAction.Request(sourceIndex, rollupIndex, config)
        ).actionGet();
        assertTrue(rollupResponse.isAcknowledged());
    }

    private RolloverResponse rollover(String dataStreamName) throws ExecutionException, InterruptedException {
        RolloverResponse response = client().admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).get();
        assertTrue(response.isAcknowledged());
        return response;
    }

    @SuppressWarnings("unchecked")
    private void assertRollupIndex(RollupActionConfig config, String sourceIndex, String rollupIndex) {
        final CompositeAggregationBuilder aggregation = buildCompositeAggs("resp", config);
        long numBuckets = 0;
        InternalComposite origResp = client().prepareSearch(sourceIndex).addAggregation(aggregation).get().getAggregations().get("resp");
        InternalComposite rollupResp = client().prepareSearch(rollupIndex).addAggregation(aggregation).get().getAggregations().get("resp");
        while (origResp.afterKey() != null) {
            numBuckets += origResp.getBuckets().size();
            assertThat(origResp, equalTo(rollupResp));
            aggregation.aggregateAfter(origResp.afterKey());
            origResp = client().prepareSearch(sourceIndex).addAggregation(aggregation).get().getAggregations().get("resp");
            rollupResp = client().prepareSearch(rollupIndex).addAggregation(aggregation).get().getAggregations().get("resp");
        }
        assertEquals(origResp, rollupResp);

        SearchResponse resp = client().prepareSearch(rollupIndex).setTrackTotalHits(true).get();
        assertThat(resp.getHits().getTotalHits().value, equalTo(numBuckets));

        GetIndexResponse indexSettingsResp = client().admin().indices().prepareGetIndex().addIndices(sourceIndex, rollupIndex).get();
        // Assert rollup metadata are set in index settings
        assertEquals(
            indexSettingsResp.getSetting(sourceIndex, "index.uuid"),
            indexSettingsResp.getSetting(rollupIndex, "index.rollup.source.uuid")
        );
        assertEquals(
            indexSettingsResp.getSetting(sourceIndex, "index.provided_name"),
            indexSettingsResp.getSetting(rollupIndex, "index.rollup.source.name")
        );
        assertEquals("time_series", indexSettingsResp.getSetting(rollupIndex, "index.mode"));

        // Assert field mappings
        Map<String, Map<String, Object>> mappings = (Map<String, Map<String, Object>>) indexSettingsResp.getMappings()
            .get(rollupIndex)
            .getSourceAsMap()
            .get("properties");

        assertEquals(DateFieldMapper.CONTENT_TYPE, mappings.get(config.getTimestampField()).get("type"));
        Map<String, Object> dateTimeMeta = (Map<String, Object>) mappings.get(config.getTimestampField()).get("meta");
        assertEquals(config.getTimeZone(), dateTimeMeta.get("time_zone"));
        assertEquals(config.getInterval().toString(), dateTimeMeta.get(config.getIntervalType()));
        //
        // for (MetricConfig metricsConfig : config.getMetricsConfig()) {
        // assertEquals("aggregate_metric_double", mappings.get(metricsConfig.getField()).get("type"));
        // List<String> supportedMetrics = (List<String>) mappings.get(metricsConfig.getField()).get("metrics");
        // for (String m : metricsConfig.getMetrics()) {
        // if ("avg".equals(m)) {
        // assertTrue(supportedMetrics.contains("sum") && supportedMetrics.contains("value_count"));
        // } else {
        // assertTrue(supportedMetrics.contains(m));
        // }
        // }
        // }
        //
        //
        //
        // TermsGroupConfig termsConfig = config.getGroupConfig().getTerms();
        // if (termsConfig != null) {
        // for (String field : termsConfig.getFields()) {
        // assertTrue(mappings.containsKey(field));
        // }
        // }

        // Assert that temporary index was removed
        expectThrows(
            IndexNotFoundException.class,
            () -> client().admin().indices().prepareGetIndex().addIndices(".rolluptmp-" + rollupIndex).get()
        );
    }

    private CompositeAggregationBuilder buildCompositeAggs(String name, RollupActionConfig config) {
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        // For time series indices, we use the _tsid field
        sources.add(new TermsValuesSourceBuilder("tsid").field("_tsid"));

        DateHistogramValuesSourceBuilder dateHisto = new DateHistogramValuesSourceBuilder("date_histo");
        dateHisto.field(config.getTimestampField());
        if (config.getTimeZone() != null) {
            dateHisto.timeZone(ZoneId.of(config.getTimeZone()));
        }
        dateHisto.fixedInterval(config.getInterval());
        sources.add(dateHisto);

        final CompositeAggregationBuilder composite = new CompositeAggregationBuilder(name, sources).size(10);

        final List<String> supportedAggs = List.of("min", "max", "sum", "value_count", "avg");

        String fieldname = "extractMetricField";
        for (String metricName : supportedAggs) {
            switch (metricName) {
                case "min" -> composite.subAggregation(new MinAggregationBuilder(metricName).field(fieldname));
                case "max" -> composite.subAggregation(new MaxAggregationBuilder(metricName).field(fieldname));
                case "sum" -> composite.subAggregation(new SumAggregationBuilder(metricName).field(fieldname));
                case "value_count" -> composite.subAggregation(new ValueCountAggregationBuilder(metricName).field(fieldname));
                case "avg" -> composite.subAggregation(new AvgAggregationBuilder(metricName).field(fieldname));
                default -> throw new IllegalArgumentException("Unsupported metric type [" + metricName + "]");
            }
        }

        return composite;
    }

    @FunctionalInterface
    public interface SourceSupplier {
        XContentBuilder get() throws IOException;
    }

    private String createDataStream() throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.getDefault());
        Template idxTemplate = new Template(null, new CompressedXContent("""
            {"properties":{"%s":{"type":"date"}, "%s":{"type":"keyword"}}}
            """.formatted(FIELD_TIMESTAMP, FIELD_CATEGORICAL_1)), null);
        ComposableIndexTemplate template = new ComposableIndexTemplate(
            List.of(dataStreamName + "*"),
            idxTemplate,
            null,
            null,
            null,
            null,
            new ComposableIndexTemplate.DataStreamTemplate(),
            null
        );
        assertTrue(
            client().execute(
                PutComposableIndexTemplateAction.INSTANCE,
                new PutComposableIndexTemplateAction.Request(dataStreamName + "_template").indexTemplate(template)
            ).actionGet().isAcknowledged()
        );
        assertTrue(
            client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)).get().isAcknowledged()
        );
        return dataStreamName;
    }
}
