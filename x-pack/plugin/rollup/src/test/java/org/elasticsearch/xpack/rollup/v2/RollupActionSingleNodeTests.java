/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionGroupConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;
import org.elasticsearch.xpack.rollup.Rollup;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/69799")
public class RollupActionSingleNodeTests extends ESSingleNodeTestCase {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private String index;
    private String rollupIndex;
    private long startTime;
    private int docCount;

    private String timestampFieldName = "@timestamp";
    private final Set<String> createdDataStreams = new HashSet<>();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(
            LocalStateCompositeXPackPlugin.class,
            Rollup.class,
            AnalyticsPlugin.class,
            AggregateMetricMapperPlugin.class,
            DataStreamsPlugin.class
        );
    }

    @Before
    public void setup() {
        index = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        rollupIndex = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        startTime = randomLongBetween(946769284000L, 1607470084000L); // random date between 2000-2020
        docCount = randomIntBetween(10, 1000);

        client().admin()
            .indices()
            .prepareCreate(index)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).build())
            .addMapping(
                "_doc",
                "date_1",
                "type=date",
                "numeric_1",
                "type=double",
                "numeric_2",
                "type=float",
                "numeric_nonaggregatable",
                "type=double,doc_values=false",
                "categorical_1",
                "type=keyword"
            )
            .get();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (createdDataStreams.isEmpty() == false) {
            for (String createdDataStream : createdDataStreams) {
                deleteDataStream(createdDataStream);
            }
            createdDataStreams.clear();
        }
        super.tearDown();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/69506")
    public void testRollupShardIndexerCleansTempFiles() throws IOException {
        // create rollup config and index documents into source index
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig = randomRollupActionDateHistogramGroupConfig("date_1");
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("categorical_1", randomAlphaOfLength(1))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, null, new TermsGroupConfig("categorical_1")),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("max")))
        );
        bulkIndex(sourceSupplier);

        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        Index srcIndex = resolveIndex(index);
        IndexService indexService = indexServices.indexServiceSafe(srcIndex);
        IndexShard shard = indexService.getShard(0);

        // re-use source index as temp index for test
        RollupShardIndexer indexer = new RollupShardIndexer(client(), indexService, shard.shardId(), config, index, 2);
        indexer.execute();
        assertThat(indexer.tmpFilesDeleted, equalTo(indexer.tmpFiles));
        // assert that files are deleted
    }

    public void testCannotRollupToExistingIndex() throws Exception {
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig = randomRollupActionDateHistogramGroupConfig("date_1");
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("categorical_1", randomAlphaOfLength(1))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, null, new TermsGroupConfig("categorical_1")),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("max")))
        );
        bulkIndex(sourceSupplier);
        rollup(index, rollupIndex, config);
        assertRollupIndex(config, index, rollupIndex);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> rollup(index, rollupIndex, config));
        assertThat(exception.getMessage(), containsString("Unable to rollup index [" + index + "]"));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/69506")
    public void testTemporaryIndexDeletedOnRollupFailure() throws Exception {
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig = randomRollupActionDateHistogramGroupConfig("date_1");
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("categorical_1", randomAlphaOfLength(1))
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, null, new TermsGroupConfig("categorical_1")),
            Collections.singletonList(new MetricConfig("numeric_non_existent", Collections.singletonList("max")))
        );
        bulkIndex(sourceSupplier);
        expectThrows(ElasticsearchException.class, () -> rollup(index, rollupIndex, config));
        // assert that temporary index was removed
        expectThrows(
            IndexNotFoundException.class,
            () -> client().admin().indices().prepareGetIndex().addIndices(".rolluptmp-" + rollupIndex).get()
        );
    }

    public void testCannotRollupWhileOtherRollupInProgress() throws Exception {
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig = randomRollupActionDateHistogramGroupConfig("date_1");
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("categorical_1", randomAlphaOfLength(1))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, null, new TermsGroupConfig("categorical_1")),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("max")))
        );
        bulkIndex(sourceSupplier);
        client().execute(RollupAction.INSTANCE, new RollupAction.Request(index, rollupIndex, config), ActionListener.wrap(() -> {}));
        ResourceAlreadyExistsException exception = expectThrows(
            ResourceAlreadyExistsException.class,
            () -> rollup(index, rollupIndex, config)
        );
        assertThat(exception.getMessage(), containsString(".rolluptmp-" + rollupIndex));
    }

    public void testTermsGrouping() throws IOException {
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig = randomRollupActionDateHistogramGroupConfig("date_1");
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("categorical_1", randomAlphaOfLength(1))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, null, new TermsGroupConfig("categorical_1")),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("max")))
        );
        bulkIndex(sourceSupplier);
        rollup(index, rollupIndex, config);
        assertRollupIndex(config, index, rollupIndex);
    }

    public void testHistogramGrouping() throws IOException {
        long interval = randomLongBetween(1, 1000);
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig = randomRollupActionDateHistogramGroupConfig("date_1");
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDoubleBetween(0.0, 10000.0, true))
            .field("numeric_2", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, new HistogramGroupConfig(interval, "numeric_1"), null),
            Collections.singletonList(new MetricConfig("numeric_2", Collections.singletonList("max")))
        );
        bulkIndex(sourceSupplier);
        rollup(index, rollupIndex, config);
        assertRollupIndex(config, index, rollupIndex);
    }

    public void testMaxMetric() throws IOException {
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig = randomRollupActionDateHistogramGroupConfig("date_1");
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("max")))
        );
        bulkIndex(sourceSupplier);
        rollup(index, rollupIndex, config);
        assertRollupIndex(config, index, rollupIndex);
    }

    public void testMinMetric() throws IOException {
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig = randomRollupActionDateHistogramGroupConfig("date_1");
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("min")))
        );
        bulkIndex(sourceSupplier);
        rollup(index, rollupIndex, config);
        assertRollupIndex(config, index, rollupIndex);
    }

    public void testValueCountMetric() throws IOException {
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig = randomRollupActionDateHistogramGroupConfig("date_1");
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("value_count")))
        );
        bulkIndex(sourceSupplier);
        rollup(index, rollupIndex, config);
        assertRollupIndex(config, index, rollupIndex);
    }

    public void testAvgMetric() throws IOException {
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig = randomRollupActionDateHistogramGroupConfig("date_1");
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            // Use integers to ensure that avg is comparable between rollup and original
            .field("numeric_1", randomInt())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("avg")))
        );
        bulkIndex(sourceSupplier);
        rollup(index, rollupIndex, config);
        assertRollupIndex(config, index, rollupIndex);
    }

    public void testValidationCheck() throws IOException {
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig = randomRollupActionDateHistogramGroupConfig("date_1");
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            // use integers to ensure that avg is comparable between rollup and original
            .field("numeric_nonaggregatable", randomInt())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_nonaggregatable", Collections.singletonList("avg")))
        );
        bulkIndex(sourceSupplier);
        Exception e = expectThrows(Exception.class, () -> rollup(index, rollupIndex, config));
        assertThat(e.getMessage(), containsString("The field [numeric_nonaggregatable] must be aggregatable"));
    }

    public void testRollupDatastream() throws Exception {
        RollupActionDateHistogramGroupConfig dateHistogramGroupConfig = randomRollupActionDateHistogramGroupConfig(timestampFieldName);
        String dataStreamName = createDataStream();

        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder()
            .startObject()
            .field(timestampFieldName, randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("value_count")))
        );
        bulkIndex(dataStreamName, sourceSupplier);

        String oldIndexName = rollover(dataStreamName).getOldIndex();
        String rollupIndexName = ".rollup-" + oldIndexName;
        rollup(oldIndexName, rollupIndexName, config);
        assertRollupIndex(config, oldIndexName, rollupIndexName);
        rollup(oldIndexName, rollupIndexName + "-2", config);
        assertRollupIndex(config, oldIndexName, rollupIndexName + "-2");
    }

    private RollupActionDateHistogramGroupConfig randomRollupActionDateHistogramGroupConfig(String field) {
        RollupActionDateHistogramGroupConfig randomConfig = ConfigTestHelpers.randomRollupActionDateHistogramGroupConfig(random());
        if (randomConfig instanceof RollupActionDateHistogramGroupConfig.FixedInterval) {
            return new RollupActionDateHistogramGroupConfig.FixedInterval(field, randomConfig.getInterval(), randomConfig.getTimeZone());
        }
        if (randomConfig instanceof RollupActionDateHistogramGroupConfig.CalendarInterval) {
            return new RollupActionDateHistogramGroupConfig.CalendarInterval(field, randomConfig.getInterval(), randomConfig.getTimeZone());
        }
        throw new IllegalStateException("invalid RollupActionDateHistogramGroupConfig class type");
    }

    private String randomDateForInterval(DateHistogramInterval interval) {
        final long maxNumBuckets = 10;
        final long endTime = startTime + maxNumBuckets * interval.estimateMillis();
        return DATE_FORMATTER.formatMillis(randomLongBetween(startTime, endTime));
    }

    private void bulkIndex(SourceSupplier sourceSupplier) throws IOException {
        bulkIndex(index, sourceSupplier);
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
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), docCount);
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
        assertThat(origResp, equalTo(rollupResp));

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

        // Assert field mappings
        Map<String, Map<String, Object>> mappings = (Map<String, Map<String, Object>>) indexSettingsResp.getMappings()
            .get(rollupIndex)
            .get("_doc")
            .getSourceAsMap()
            .get("properties");

        RollupActionDateHistogramGroupConfig dateHistoConfig = config.getGroupConfig().getDateHistogram();
        assertEquals(DateFieldMapper.CONTENT_TYPE, mappings.get(dateHistoConfig.getField()).get("type"));
        Map<String, Object> dateTimeMeta = (Map<String, Object>) mappings.get(dateHistoConfig.getField()).get("meta");
        assertEquals(dateHistoConfig.getTimeZone(), dateTimeMeta.get("time_zone"));
        assertEquals(dateHistoConfig.getInterval().toString(), dateTimeMeta.get(dateHistoConfig.getIntervalTypeName()));

        for (MetricConfig metricsConfig : config.getMetricsConfig()) {
            assertEquals("aggregate_metric_double", mappings.get(metricsConfig.getField()).get("type"));
            List<String> supportedMetrics = (List<String>) mappings.get(metricsConfig.getField()).get("metrics");
            for (String m : metricsConfig.getMetrics()) {
                if ("avg".equals(m)) {
                    assertTrue(supportedMetrics.contains("sum") && supportedMetrics.contains("value_count"));
                } else {
                    assertTrue(supportedMetrics.contains(m));
                }
            }
        }

        HistogramGroupConfig histoConfig = config.getGroupConfig().getHistogram();
        if (histoConfig != null) {
            for (String field : histoConfig.getFields()) {
                assertTrue((mappings.containsKey(field)));
                Map<String, Object> meta = (Map<String, Object>) mappings.get(field).get("meta");
                assertEquals(String.valueOf(histoConfig.getInterval()), meta.get("interval"));
            }
        }

        TermsGroupConfig termsConfig = config.getGroupConfig().getTerms();
        if (termsConfig != null) {
            for (String field : termsConfig.getFields()) {
                assertTrue(mappings.containsKey(field));
            }
        }

        // Assert that temporary index was removed
        expectThrows(
            IndexNotFoundException.class,
            () -> client().admin().indices().prepareGetIndex().addIndices(".rolluptmp-" + rollupIndex).get()
        );
    }

    private CompositeAggregationBuilder buildCompositeAggs(String name, RollupActionConfig config) {
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();

        RollupActionDateHistogramGroupConfig dateHistoConfig = config.getGroupConfig().getDateHistogram();
        DateHistogramValuesSourceBuilder dateHisto = new DateHistogramValuesSourceBuilder(dateHistoConfig.getField());
        dateHisto.field(dateHistoConfig.getField());
        if (dateHistoConfig.getTimeZone() != null) {
            dateHisto.timeZone(ZoneId.of(dateHistoConfig.getTimeZone()));
        }
        if (dateHistoConfig instanceof RollupActionDateHistogramGroupConfig.FixedInterval) {
            dateHisto.fixedInterval(dateHistoConfig.getInterval());
        } else if (dateHistoConfig instanceof RollupActionDateHistogramGroupConfig.CalendarInterval) {
            dateHisto.calendarInterval(dateHistoConfig.getInterval());
        } else {
            throw new IllegalStateException("unsupported RollupActionDateHistogramGroupConfig");
        }
        sources.add(dateHisto);

        if (config.getGroupConfig().getHistogram() != null) {
            HistogramGroupConfig histoConfig = config.getGroupConfig().getHistogram();
            for (String field : histoConfig.getFields()) {
                HistogramValuesSourceBuilder source = new HistogramValuesSourceBuilder(field).field(field)
                    .interval(histoConfig.getInterval());
                sources.add(source);
            }
        }

        if (config.getGroupConfig().getTerms() != null) {
            TermsGroupConfig termsConfig = config.getGroupConfig().getTerms();
            for (String field : termsConfig.getFields()) {
                TermsValuesSourceBuilder source = new TermsValuesSourceBuilder(field).field(field);
                sources.add(source);
            }
        }

        final CompositeAggregationBuilder composite = new CompositeAggregationBuilder(name, sources).size(100);
        if (config.getMetricsConfig() != null) {
            for (MetricConfig metricConfig : config.getMetricsConfig()) {
                for (String metricName : metricConfig.getMetrics()) {
                    switch (metricName) {
                        case "min":
                            composite.subAggregation(new MinAggregationBuilder(metricName).field(metricConfig.getField()));
                            break;
                        case "max":
                            composite.subAggregation(new MaxAggregationBuilder(metricName).field(metricConfig.getField()));
                            break;
                        case "sum":
                            composite.subAggregation(new SumAggregationBuilder(metricName).field(metricConfig.getField()));
                            break;
                        case "value_count":
                            composite.subAggregation(new ValueCountAggregationBuilder(metricName).field(metricConfig.getField()));
                            break;
                        case "avg":
                            composite.subAggregation(new AvgAggregationBuilder(metricName).field(metricConfig.getField()));
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported metric type [" + metricName + "]");
                    }
                }
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
        Template idxTemplate = new Template(
            null,
            new CompressedXContent("{\"properties\":{\"" + timestampFieldName + "\":{\"type\":\"date\"},\"data\":{\"type\":\"keyword\"}}}"),
            null
        );
        ComposableIndexTemplate template = new ComposableIndexTemplate(
            org.elasticsearch.core.List.of(dataStreamName + "*"),
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
        createdDataStreams.add(dataStreamName);
        return dataStreamName;
    }

    private void deleteDataStream(String dataStreamName) throws InterruptedException, java.util.concurrent.ExecutionException {
        assertTrue(
            client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { dataStreamName }))
                .get()
                .isAcknowledged()
        );
        assertTrue(
            client().execute(
                DeleteComposableIndexTemplateAction.INSTANCE,
                new DeleteComposableIndexTemplateAction.Request(dataStreamName + "_template")
            ).actionGet().isAcknowledged()
        );
    }
}
