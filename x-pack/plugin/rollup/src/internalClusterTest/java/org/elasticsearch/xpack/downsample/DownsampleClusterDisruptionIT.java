/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;
import org.elasticsearch.xpack.rollup.Rollup;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomInterval;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 4)
public class DownsampleClusterDisruptionIT extends ESIntegTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);
    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    public static final String FIELD_TIMESTAMP = "@timestamp";
    public static final String FIELD_DIMENSION_1 = "dimension_kw";
    public static final String FIELD_DIMENSION_2 = "dimension_long";

    public static final String FIELD_METRIC_LABEL_DOUBLE = "metric_label_double";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, Rollup.class, AggregateMetricMapperPlugin.class);
    }

    public void setup(final String sourceIndex, int numOfShards, int numOfReplicas, long startTime) throws IOException {
        Settings.Builder settings = indexSettings(numOfShards, numOfReplicas).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1))
            .put(
                IndexSettings.TIME_SERIES_START_TIME.getKey(),
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(Instant.ofEpochMilli(startTime).toEpochMilli())
            )
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z");

        if (randomBoolean()) {
            settings.put(IndexMetadata.SETTING_INDEX_HIDDEN, randomBoolean());
        }

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");
        mapping.startObject(FIELD_TIMESTAMP).field("type", "date").endObject();

        // Dimensions
        mapping.startObject(FIELD_DIMENSION_1).field("type", "keyword").field("time_series_dimension", true).endObject();
        mapping.startObject(FIELD_DIMENSION_2).field("type", "long").field("time_series_dimension", true).endObject();

        // Metrics
        mapping.startObject(FIELD_METRIC_LABEL_DOUBLE)
            .field("type", "double") /* numeric label indexed as a metric */
            .field("time_series_metric", "counter")
            .endObject();

        // Labels
        mapping.endObject().endObject().endObject();
        assertAcked(indicesAdmin().prepareCreate(sourceIndex).setSettings(settings.build()).setMapping(mapping).get());
    }

    public void testRollupIndexWithDataNodeDisruptions() throws IOException, InterruptedException {
        final List<String> masterNodes = internalCluster().startMasterOnlyNodes(1);
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(3);
        final String[] allNodes = internalCluster().getNodeNames();
        ensureStableCluster(internalCluster().size());
        ensureGreen();

        assertEquals(masterNodes.size() + dataNodes.size(), Arrays.stream(allNodes).count());
        assertEquals(Arrays.stream(allNodes).count(), internalCluster().size());
        assertFalse(dataNodes.contains(masterNodes));
        assertFalse(masterNodes.contains(dataNodes));

        final String sourceIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String rollupIndex = randomAlphaOfLength(11).toLowerCase(Locale.ROOT);
        long startTime = LocalDateTime.parse("2020-09-09T18:00:00").atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        // NOTE: we need replicas otherwise we might restart the node with the only source index shard and the index would not be available
        setup(sourceIndex, 1, 0, startTime);
        final DownsampleConfig config = new DownsampleConfig(randomInterval(), TIMEOUT);
        final DownsampleActionSingleNodeTests.SourceSupplier sourceSupplier = () -> {
            final String ts = randomDateForInterval(config.getInterval(), startTime);
            double labelDoubleValue = DATE_FORMATTER.parseMillis(ts);
            final List<String> dimensionValues = new ArrayList<>(5);
            for (int j = 0; j < randomIntBetween(1, 5); j++) {
                dimensionValues.add(randomAlphaOfLength(6));
            }
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                .field(FIELD_DIMENSION_2, randomIntBetween(1, 10))
                .field(FIELD_METRIC_LABEL_DOUBLE, labelDoubleValue)
                .endObject();
        };
        bulkIndex(sourceIndex, sourceSupplier, 9_000);
        prepareSourceIndex(sourceIndex);
        CountDownLatch disruptionStart = new CountDownLatch(1);
        CountDownLatch disruptionEnd = new CountDownLatch(1);

        new Thread(() -> {
            disruptionStart.countDown();
            try {
                ensureGreen(sourceIndex);
                ensureStableCluster(internalCluster().size());
                final String restartingNode = client().admin().cluster().prepareSearchShards(sourceIndex).get().getNodes()[0].getName();
                logger.info("Restarting random data node [" + restartingNode + "]");
                try {
                    internalCluster().restartNode(restartingNode);
                    ensureGreen(sourceIndex);
                    ensureStableCluster(internalCluster().numDataAndMasterNodes(), masterNodes.get(0));
                } catch (Exception e) {
                    logger.warn("Ignoring exception while trying to restart node [" + restartingNode + "]");
                }
                disruptionEnd.countDown();

            } catch (Exception e) {
                logger.error("Error while injecting disruption [" + e.getMessage() + "]");
            }
        }).start();

        disruptionStart.await();
        rollup(sourceIndex, rollupIndex, config);
        disruptionEnd.await();
        ensureStableCluster(internalCluster().numDataAndMasterNodes());
        assertRollupIndex(sourceIndex, rollupIndex, internalCluster().dataNodeClient());
    }

    private void assertRollupIndex(final String sourceIndex, final String rollupIndex, final Client client) throws IOException {
        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(rollupIndex)).actionGet();
        assertEquals(1, getIndexResponse.indices().length);
        final SearchResponse searchResponse = client.prepareSearch(rollupIndex)
            .setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE)
            .get();
    }

    private void bulkIndex(final String indexName, final DownsampleActionSingleNodeTests.SourceSupplier sourceSupplier, int docCount)
        throws IOException {
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
        assertAcked(
            indicesAdmin().prepareUpdateSettings(sourceIndex)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build())
                .get()
        );
    }

    private void rollup(String sourceIndex, String rollupIndex, DownsampleConfig config) {
        assertAcked(
            client().execute(DownsampleAction.INSTANCE, new DownsampleAction.Request(sourceIndex, rollupIndex, config)).actionGet()
        );
    }

    private String randomDateForInterval(final DateHistogramInterval interval, final long startTime) {
        long endTime = startTime + 10 * interval.estimateMillis();
        return randomDateForRange(startTime, endTime);
    }

    private String randomDateForRange(long start, long end) {
        return DATE_FORMATTER.formatMillis(randomLongBetween(start, end));
    }
}
