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
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomInterval;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 4)
public class ILMDownsampleDisruptionIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(ILMDownsampleDisruptionIT.class);
    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private static final String FIELD_TIMESTAMP = "@timestamp";
    private static final String FIELD_DIMENSION_1 = "dimension_kw";
    private static final String FIELD_DIMENSION_2 = "dimension_long";
    private static final String FIELD_METRIC_COUNTER = "counter";
    private static final String POLICY_NAME = "mypolicy";
    public static final int DOC_COUNT = 10_000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            LocalStateCompositeXPackPlugin.class,
            Downsample.class,
            AggregateMetricMapperPlugin.class,
            LocalStateCompositeXPackPlugin.class,
            IndexLifecycle.class,
            Ccr.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder nodeSettings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        nodeSettings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");

        // This is necessary to prevent ILM installing a lifecycle policy, these tests assume a blank slate
        nodeSettings.put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false);
        return nodeSettings.build();
    }

    public void setup(final String sourceIndex, int numOfShards, int numOfReplicas, long startTime) throws IOException {
        final Settings.Builder settings = indexSettings(numOfShards, numOfReplicas).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of(FIELD_DIMENSION_1))
            .put(
                IndexSettings.TIME_SERIES_START_TIME.getKey(),
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(Instant.ofEpochMilli(startTime).toEpochMilli())
            )
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2022-01-08T23:40:53.384Z");

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

        Map<String, Phase> phases = new HashMap<>();
        phases.put(
            "warm",
            new Phase(
                "warm",
                TimeValue.ZERO,
                Map.of("downsample", new org.elasticsearch.xpack.core.ilm.DownsampleAction(DateHistogramInterval.HOUR, null))
            )
        );
        LifecyclePolicy policy = new LifecyclePolicy(POLICY_NAME, phases);
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, policy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).actionGet());
    }

    public void testILMDownsampleRollingRestart() throws Exception {
        final InternalTestCluster cluster = internalCluster();
        final List<String> masterNodes = cluster.startMasterOnlyNodes(1);
        cluster.startDataOnlyNodes(3);
        ensureStableCluster(cluster.size());
        ensureGreen();

        final String sourceIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        long startTime = LocalDateTime.parse("1993-09-09T18:00:00").atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        setup(sourceIndex, 1, 0, startTime);
        final DownsampleConfig config = new DownsampleConfig(randomInterval());
        final SourceSupplier sourceSupplier = () -> {
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

        final String targetIndex = "downsample-1h-" + sourceIndex;
        startDownsampleTaskViaIlm(sourceIndex, targetIndex, disruptionStart, disruptionEnd);
        waitUntil(() -> getClusterPendingTasks(cluster.client()).pendingTasks().isEmpty(), 60, TimeUnit.SECONDS);
        ensureStableCluster(cluster.numDataAndMasterNodes());
        assertTargetIndex(cluster, targetIndex, indexedDocs);
    }

    private void startDownsampleTaskViaIlm(
        String sourceIndex,
        String targetIndex,
        CountDownLatch disruptionStart,
        CountDownLatch disruptionEnd
    ) throws Exception {
        disruptionStart.await();
        var request = new UpdateSettingsRequest(sourceIndex).settings(
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, POLICY_NAME)
        );
        // Updating index.lifecycle.name setting may fail due to the rolling restart itself,
        // we need to attempt it in a assertBusy(...)
        assertBusy(() -> {
            try {
                if (indexExists(sourceIndex) == false) {
                    logger.info("The source index [{}] no longer exists, downsampling likely completed", sourceIndex);
                    return;
                }
                client().admin().indices().updateSettings(request).actionGet(TimeValue.timeValueSeconds(10));
            } catch (Exception e) {
                logger.warn(() -> format("encountered failure while updating [%s] index's ilm policy", sourceIndex), e);
                throw new AssertionError(e);
            }
        }, 1, TimeUnit.MINUTES);
        assertBusy(() -> {
            assertTrue("target index [" + targetIndex + "] does not exist", indexExists(targetIndex));
            var getSettingsResponse = client().admin().indices().getSettings(new GetSettingsRequest().indices(targetIndex)).actionGet();
            assertThat(getSettingsResponse.getSetting(targetIndex, IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey()), equalTo("success"));
        }, 60, TimeUnit.SECONDS);
        disruptionEnd.await();
    }

    private void assertTargetIndex(final InternalTestCluster cluster, final String targetIndex, int indexedDocs) {
        final GetIndexResponse getIndexResponse = cluster.client()
            .admin()
            .indices()
            .getIndex(new GetIndexRequest().indices(targetIndex))
            .actionGet();
        assertEquals(1, getIndexResponse.indices().length);
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

    private int bulkIndex(final String indexName, final SourceSupplier sourceSupplier, int docCount) throws IOException {
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

    private String randomDateForInterval(final DateHistogramInterval interval, final long startTime) {
        long endTime = startTime + 10 * interval.estimateMillis();
        return randomDateForRange(startTime, endTime);
    }

    private String randomDateForRange(long start, long end) {
        return DATE_FORMATTER.formatMillis(randomLongBetween(start, end));
    }

    @FunctionalInterface
    public interface SourceSupplier {
        XContentBuilder get() throws IOException;
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
                final String candidateNode = safeExecute(
                    cluster.client(clientNode),
                    TransportClusterSearchShardsAction.TYPE,
                    new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT, sourceIndex)
                ).getNodes()[0].getName();
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
}
