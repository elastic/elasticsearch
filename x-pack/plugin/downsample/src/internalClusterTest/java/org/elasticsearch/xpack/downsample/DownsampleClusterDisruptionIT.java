/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
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
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomInterval;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 4)
public class DownsampleClusterDisruptionIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(DownsampleClusterDisruptionIT.class);
    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private static final TimeValue TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);
    public static final String FIELD_TIMESTAMP = "@timestamp";
    public static final String FIELD_DIMENSION_1 = "dimension_kw";
    public static final String FIELD_DIMENSION_2 = "dimension_long";
    public static final String FIELD_METRIC_COUNTER = "counter";
    public static final int DOC_COUNT = 10_000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, Downsample.class, AggregateMetricMapperPlugin.class);
    }

    interface DisruptionListener {
        void disruptionStart();

        void disruptionEnd();
    }

    private class Disruptor implements Runnable {
        final InternalTestCluster cluster;
        private final String sourceIndex;
        private final DisruptionListener listener;
        private final String clientNode;
        private final Consumer<String> disruption;

        private Disruptor(
            final InternalTestCluster cluster,
            final String sourceIndex,
            final DisruptionListener listener,
            final String clientNode,
            final Consumer<String> disruption
        ) {
            this.cluster = cluster;
            this.sourceIndex = sourceIndex;
            this.listener = listener;
            this.clientNode = clientNode;
            this.disruption = disruption;
        }

        @Override
        public void run() {
            listener.disruptionStart();
            try {
                final String candidateNode = cluster.client(clientNode)
                    .admin()
                    .cluster()
                    .prepareSearchShards(sourceIndex)
                    .get()
                    .getNodes()[0].getName();
                logger.info("Candidate node [" + candidateNode + "]");
                disruption.accept(candidateNode);
                ensureGreen(sourceIndex);
                ensureStableCluster(cluster.numDataAndMasterNodes(), clientNode);

            } catch (Exception e) {
                logger.error("Ignoring Error while injecting disruption [" + e.getMessage() + "]");
            } finally {
                listener.disruptionEnd();
            }
        }
    }

    public void setup(final String sourceIndex, int numOfShards, int numOfReplicas, long startTime) throws IOException {
        final Settings.Builder settings = indexSettings(numOfShards, numOfReplicas).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1))
            .put(
                IndexSettings.TIME_SERIES_START_TIME.getKey(),
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(Instant.ofEpochMilli(startTime).toEpochMilli())
            )
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2106-01-08T23:40:53.384Z");

        if (randomBoolean()) {
            settings.put(IndexMetadata.SETTING_INDEX_HIDDEN, randomBoolean());
        }

        final XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");
        mapping.startObject(FIELD_TIMESTAMP).field("type", "date").endObject();

        mapping.startObject(FIELD_DIMENSION_1).field("type", "keyword").field("time_series_dimension", true).endObject();
        mapping.startObject(FIELD_DIMENSION_2).field("type", "long").field("time_series_dimension", true).endObject();

        mapping.startObject(FIELD_METRIC_COUNTER)
            .field("type", "double") /* numeric label indexed as a metric */
            .field("time_series_metric", "counter")
            .endObject();

        mapping.endObject().endObject().endObject();
        assertAcked(indicesAdmin().prepareCreate(sourceIndex).setSettings(settings.build()).setMapping(mapping).get());
    }

    public void testDownsampleIndexWithDataNodeRestart() throws Exception {
        final InternalTestCluster cluster = internalCluster();
        final List<String> masterNodes = cluster.startMasterOnlyNodes(1);
        cluster.startDataOnlyNodes(3);
        ensureStableCluster(cluster.size());
        ensureGreen();

        final String sourceIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String targetIndex = randomAlphaOfLength(11).toLowerCase(Locale.ROOT);
        long startTime = LocalDateTime.parse("2020-09-09T18:00:00").atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        setup(sourceIndex, 1, 0, startTime);
        final DownsampleConfig config = new DownsampleConfig(randomInterval());
        final DownsampleActionSingleNodeTests.SourceSupplier sourceSupplier = () -> {
            final String ts = randomDateForInterval(config.getInterval(), startTime);
            double counterValue = DATE_FORMATTER.parseMillis(ts);
            final List<String> dimensionValues = new ArrayList<>(5);
            for (int j = 0; j < randomIntBetween(1, 5); j++) {
                dimensionValues.add(randomAlphaOfLength(6));
            }
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                .field(FIELD_DIMENSION_2, randomIntBetween(1, 10))
                .field(FIELD_METRIC_COUNTER, counterValue)
                .endObject();
        };
        int indexedDocs = bulkIndex(sourceIndex, sourceSupplier, DOC_COUNT);
        prepareSourceIndex(sourceIndex);
        final CountDownLatch disruptionStart = new CountDownLatch(1);
        final CountDownLatch disruptionEnd = new CountDownLatch(1);

        new Thread(new Disruptor(cluster, sourceIndex, new DisruptionListener() {
            @Override
            public void disruptionStart() {
                disruptionStart.countDown();
            }

            @Override
            public void disruptionEnd() {
                disruptionEnd.countDown();
            }
        }, masterNodes.get(0), (node) -> {
            try {
                cluster.restartNode(node, new InternalTestCluster.RestartCallback() {
                    @Override
                    public boolean validateClusterForming() {
                        return true;
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        })).start();
        startDownsampleTaskDuringDisruption(sourceIndex, targetIndex, config, disruptionStart, disruptionEnd);
        waitUntil(() -> cluster.client().admin().cluster().preparePendingClusterTasks().get().pendingTasks().isEmpty());
        ensureStableCluster(cluster.numDataAndMasterNodes());
        assertTargetIndex(cluster, sourceIndex, targetIndex, indexedDocs);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100653")
    public void testDownsampleIndexWithRollingRestart() throws Exception {
        final InternalTestCluster cluster = internalCluster();
        final List<String> masterNodes = cluster.startMasterOnlyNodes(1);
        cluster.startDataOnlyNodes(3);
        ensureStableCluster(cluster.size());
        ensureGreen();

        final String sourceIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String targetIndex = randomAlphaOfLength(11).toLowerCase(Locale.ROOT);
        long startTime = LocalDateTime.parse("2020-09-09T18:00:00").atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        setup(sourceIndex, 1, 0, startTime);
        final DownsampleConfig config = new DownsampleConfig(randomInterval());
        final DownsampleActionSingleNodeTests.SourceSupplier sourceSupplier = () -> {
            final String ts = randomDateForInterval(config.getInterval(), startTime);
            double counterValue = DATE_FORMATTER.parseMillis(ts);
            final List<String> dimensionValues = new ArrayList<>(5);
            for (int j = 0; j < randomIntBetween(1, 5); j++) {
                dimensionValues.add(randomAlphaOfLength(6));
            }
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                .field(FIELD_DIMENSION_2, randomIntBetween(1, 10))
                .field(FIELD_METRIC_COUNTER, counterValue)
                .endObject();
        };
        int indexedDocs = bulkIndex(sourceIndex, sourceSupplier, DOC_COUNT);
        prepareSourceIndex(sourceIndex);
        final CountDownLatch disruptionStart = new CountDownLatch(1);
        final CountDownLatch disruptionEnd = new CountDownLatch(1);

        new Thread(new Disruptor(cluster, sourceIndex, new DisruptionListener() {
            @Override
            public void disruptionStart() {
                disruptionStart.countDown();
            }

            @Override
            public void disruptionEnd() {
                disruptionEnd.countDown();
            }
        }, masterNodes.get(0), (ignored) -> {
            try {
                cluster.rollingRestart(new InternalTestCluster.RestartCallback() {
                    @Override
                    public boolean validateClusterForming() {
                        return true;
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        })).start();

        startDownsampleTaskDuringDisruption(sourceIndex, targetIndex, config, disruptionStart, disruptionEnd);
        waitUntil(() -> cluster.client().admin().cluster().preparePendingClusterTasks().get().pendingTasks().isEmpty());
        ensureStableCluster(cluster.numDataAndMasterNodes());
        assertTargetIndex(cluster, sourceIndex, targetIndex, indexedDocs);
    }

    /**
     * Starts a downsample operation.
     *
     * @param sourceIndex the idex to read data from
     * @param targetIndex the idnex to write downsampled data to
     * @param config the downsample configuration including the downsample granularity
     * @param disruptionStart a latch to synchronize on the disruption starting
     * @param disruptionEnd a latch to synchronize on the disruption ending
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    private void startDownsampleTaskDuringDisruption(
        final String sourceIndex,
        final String targetIndex,
        final DownsampleConfig config,
        final CountDownLatch disruptionStart,
        final CountDownLatch disruptionEnd
    ) throws Exception {
        disruptionStart.await();
        assertBusy(() -> {
            try {
                downsample(sourceIndex, targetIndex, config);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, 120, TimeUnit.SECONDS);
        disruptionEnd.await();
    }

    public void testDownsampleIndexWithFullClusterRestart() throws Exception {
        final InternalTestCluster cluster = internalCluster();
        final List<String> masterNodes = cluster.startMasterOnlyNodes(1);
        cluster.startDataOnlyNodes(3);
        ensureStableCluster(cluster.size());
        ensureGreen();

        final String sourceIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String downsampleIndex = randomAlphaOfLength(11).toLowerCase(Locale.ROOT);
        long startTime = LocalDateTime.parse("2020-09-09T18:00:00").atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        setup(sourceIndex, 1, 0, startTime);
        final DownsampleConfig config = new DownsampleConfig(randomInterval());
        final DownsampleActionSingleNodeTests.SourceSupplier sourceSupplier = () -> {
            final String ts = randomDateForInterval(config.getInterval(), startTime);
            double counterValue = DATE_FORMATTER.parseMillis(ts);
            final List<String> dimensionValues = new ArrayList<>(5);
            for (int j = 0; j < randomIntBetween(1, 5); j++) {
                dimensionValues.add(randomAlphaOfLength(6));
            }
            return XContentFactory.jsonBuilder()
                .startObject()
                .field(FIELD_TIMESTAMP, ts)
                .field(FIELD_DIMENSION_1, randomFrom(dimensionValues))
                .field(FIELD_DIMENSION_2, randomIntBetween(1, 10))
                .field(FIELD_METRIC_COUNTER, counterValue)
                .endObject();
        };
        int indexedDocs = bulkIndex(sourceIndex, sourceSupplier, DOC_COUNT);
        prepareSourceIndex(sourceIndex);
        final CountDownLatch disruptionStart = new CountDownLatch(1);
        final CountDownLatch disruptionEnd = new CountDownLatch(1);

        new Thread(new Disruptor(cluster, sourceIndex, new DisruptionListener() {
            @Override
            public void disruptionStart() {
                disruptionStart.countDown();
            }

            @Override
            public void disruptionEnd() {
                disruptionEnd.countDown();
            }
        }, masterNodes.get(0), (ignored) -> {
            try {
                cluster.fullRestart(new InternalTestCluster.RestartCallback() {
                    @Override
                    public boolean validateClusterForming() {
                        return true;
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        })).start();

        startDownsampleTaskDuringDisruption(sourceIndex, downsampleIndex, config, disruptionStart, disruptionEnd);
        waitUntil(() -> cluster.client().admin().cluster().preparePendingClusterTasks().get().pendingTasks().isEmpty());
        ensureStableCluster(cluster.numDataAndMasterNodes());
        assertTargetIndex(cluster, sourceIndex, downsampleIndex, indexedDocs);
    }

    private void assertTargetIndex(final InternalTestCluster cluster, final String sourceIndex, final String targetIndex, int indexedDocs) {
        final GetIndexResponse getIndexResponse = cluster.client()
            .admin()
            .indices()
            .getIndex(new GetIndexRequest().indices(targetIndex))
            .actionGet();
        assertEquals(1, getIndexResponse.indices().length);
        assertResponse(
            cluster.client()
                .prepareSearch(sourceIndex)
                .setQuery(new MatchAllQueryBuilder())
                .setSize(Math.min(DOC_COUNT, indexedDocs))
                .setTrackTotalHitsUpTo(Integer.MAX_VALUE),
            sourceIndexSearch -> {
                assertEquals(indexedDocs, sourceIndexSearch.getHits().getHits().length);
            }
        );
        assertResponse(
            cluster.client()
                .prepareSearch(targetIndex)
                .setQuery(new MatchAllQueryBuilder())
                .setSize(Math.min(DOC_COUNT, indexedDocs))
                .setTrackTotalHitsUpTo(Integer.MAX_VALUE),
            targetIndexSearch -> {
                assertTrue(targetIndexSearch.getHits().getHits().length > 0);
            }
        );
    }

    private int bulkIndex(final String indexName, final DownsampleActionSingleNodeTests.SourceSupplier sourceSupplier, int docCount)
        throws IOException {
        BulkRequestBuilder bulkRequestBuilder = internalCluster().client().prepareBulk();
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
        return docsIndexed;
    }

    private void prepareSourceIndex(String sourceIndex) {
        // Set the source index to read-only state
        assertAcked(
            indicesAdmin().prepareUpdateSettings(sourceIndex)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build())
        );
    }

    private void downsample(final String sourceIndex, final String downsampleIndex, final DownsampleConfig config) {
        assertAcked(
            internalCluster().client()
                .execute(DownsampleAction.INSTANCE, new DownsampleAction.Request(sourceIndex, downsampleIndex, TIMEOUT, config))
                .actionGet(TIMEOUT.millis())
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
