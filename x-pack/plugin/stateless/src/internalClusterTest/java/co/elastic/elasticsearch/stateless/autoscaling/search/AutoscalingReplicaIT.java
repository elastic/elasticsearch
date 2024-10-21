/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class AutoscalingReplicaIT extends AbstractStatelessIntegTestCase {

    private static final long DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(7).millis();
    private static final long ONE_DAY = TimeValue.timeValueDays(1).millis();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    public void testSearchPowerAffectsReplica() throws Exception {
        Settings settings = Settings.builder()
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(250))
            .put(ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getKey(), TimeValue.timeValueMillis(100))
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true)
            .build();
        startMasterOnlyNode(settings);
        startIndexNode(settings);
        startSearchNode(settings);

        var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());

        // new documents should count towards non-interactive part
        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(
            indexName,
            100,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(indexName);
        assertEquals(1, clusterService.state().metadata().index(indexName).getNumberOfReplicas());
        int searchPowerOver250 = randomIntBetween(
            ReplicasUpdaterService.SEARCH_POWER_MIN_FULL_REPLICATION,
            ReplicasUpdaterService.SEARCH_POWER_MIN_FULL_REPLICATION + 100
        );
        assertAcked(
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
                        Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), searchPowerOver250).build()
                    )
                )
                .get()
        );
        waitUntil(() -> clusterService.state().metadata().index(indexName).getNumberOfReplicas() == 2, 5, TimeUnit.SECONDS);
        assertEquals(2, clusterService.state().metadata().index(indexName).getNumberOfReplicas());

        // also check that a newly created index gets scaled up automatically
        var indexName2 = randomIdentifier();
        createIndex(indexName2, indexSettings(5, 1).build());
        assertEquals(1, clusterService.state().metadata().index(indexName2).getNumberOfReplicas());

        indexDocumentsWithTimestamp(
            indexName2,
            1,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(indexName2);
        waitUntil(() -> clusterService.state().metadata().index(indexName2).getNumberOfReplicas() == 2, 5, TimeUnit.SECONDS);
        assertEquals(2, clusterService.state().metadata().index(indexName2).getNumberOfReplicas());

        // back to SP 100
        assertAcked(
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
                        Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 100).build()
                    )
                )
                .get()
        );
        waitUntil(() -> clusterService.state().metadata().index(indexName).getNumberOfReplicas() == 1, 5, TimeUnit.SECONDS);
        assertEquals(1, clusterService.state().metadata().index(indexName).getNumberOfReplicas());
    }

    public void testSearchSizeAffectsReplicasSPBetween100And250() throws Exception {
        // start the nodes with replica autoscaling disabled, we switch it on later
        Settings settings = Settings.builder()
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(250))
            .put(ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getKey(), TimeValue.timeValueMillis(300))
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), false)
            .build();
        startMasterOnlyNode(settings);
        startIndexNode(settings);
        startSearchNode(settings);

        var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);

        // create two indices, where size index1 is roughly 2/3 and index2 is roughly 1/3 of interactive size
        var index1 = "index1";
        createIndex(index1, indexSettings(1, 1).build());
        var index2 = "index2";
        createIndex(index2, indexSettings(1, 1).build());

        // new documents should count towards non-interactive part
        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(
            index1,
            200,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(index1);

        indexDocumentsWithTimestamp(
            index2,
            100,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(index2);

        // we need to wait until we have received shard size updates in the search metrics service
        waitUntil(() -> {
            ConcurrentMap<Index, SearchMetricsService.IndexProperties> indices = searchMetricsService.getIndices();
            boolean bothInteractiveSizePresent = true;
            for (Index i : indices.keySet()) {
                SearchMetricsService.ShardMetrics shardMetric = searchMetricsService.getShardMetrics().get(new ShardId(i, 0));
                if (shardMetric.shardSize.interactiveSizeInBytes() == 0) {
                    bothInteractiveSizePresent = false;
                }
            }
            return bothInteractiveSizePresent;
        }, 2, TimeUnit.SECONDS);

        assertEquals(1, clusterService.state().metadata().index(index1).getNumberOfReplicas());
        assertEquals(1, clusterService.state().metadata().index(index2).getNumberOfReplicas());

        // switch on relica autoscaling and set SP to 220, which should allow index1 to get two replicas
        assertAcked(
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
                        Settings.builder()
                            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 220)
                            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true)
                            .build()
                    )
                )
                .get()
        );
        // scaling up should happen almost immediately
        waitUntil(() -> clusterService.state().metadata().index(index1).getNumberOfReplicas() == 2, 1, TimeUnit.SECONDS);
        assertEquals(1, clusterService.state().metadata().index(index2).getNumberOfReplicas());
        assertEquals(2, clusterService.state().metadata().index(index1).getNumberOfReplicas());

        // indexing into index2 so that his index now has roughly 2/3 size of total interactive size
        // index1 has 200 docs, index 2 already 100, so we need another 300
        indexDocumentsWithTimestamp(
            index2,
            300,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(index2);

        // scaling up index2 should happen almost immediately, but we wait 1sec to be sure we catch at least one update interval
        waitUntil(() -> clusterService.state().metadata().index(index2).getNumberOfReplicas() == 2, 1, TimeUnit.SECONDS);
        assertEquals(2, clusterService.state().metadata().index(index2).getNumberOfReplicas());
        // index1 should still have 2 replicas, it needs 6*500ms for the change to stabiliza
        assertEquals(2, clusterService.state().metadata().index(index1).getNumberOfReplicas());

        waitUntil(() -> clusterService.state().metadata().index(index1).getNumberOfReplicas() == 1, 4, TimeUnit.SECONDS);
        assertEquals(1, clusterService.state().metadata().index(index1).getNumberOfReplicas());
    }

    public void testDisablingReplicasScalesDown() throws Exception {
        // start in a state with an index scaled to two replicas
        Settings settings = Settings.builder()
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(250))
            .put(ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getKey(), TimeValue.timeValueMillis(100))
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true)
            .build();
        startMasterOnlyNode(settings);
        startIndexNode(settings);
        startSearchNode(settings);

        var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());

        // new documents should count towards non-interactive part
        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(
            indexName,
            100,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(indexName);
        assertEquals(1, clusterService.state().metadata().index(indexName).getNumberOfReplicas());
        assertAcked(
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
                        Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 250).build()
                    )
                )
                .get()
        );
        waitUntil(() -> clusterService.state().metadata().index(indexName).getNumberOfReplicas() == 2, 2, TimeUnit.SECONDS);
        assertEquals(2, clusterService.state().metadata().index(indexName).getNumberOfReplicas());

        // now disable feature
        setFeatureFlag(false);
        waitUntil(() -> clusterService.state().metadata().index(indexName).getNumberOfReplicas() == 1, 2, TimeUnit.SECONDS);
        assertEquals(1, clusterService.state().metadata().index(indexName).getNumberOfReplicas());
    }

    public void testMultipleDatastreamsRanking() throws Exception {
        // setup with auto replica selection disabled
        Settings settings = Settings.builder()
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(250))
            .put(ReplicasUpdaterService.REPLICA_UPDATER_INTERVAL.getKey(), TimeValue.timeValueMillis(100))
            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 205)
            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), false)
            .build();
        startMasterOnlyNode(settings);
        startIndexNode(settings);
        startSearchNode(settings);

        var cs = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        var sms = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);

        putComposableIndexTemplate(
            "my-template",
            List.of("logs-*"),
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );

        final String dataStream1 = "logs-es1";
        setupDataStream(dataStream1);
        final String dataStream2 = "logs-es2";
        setupDataStream(dataStream2);
        ensureGreen();

        // ensure we see stats for all 4 backing indices in SearchMetricsService and that we they all have interactive data
        assertTrue(waitUntil(() -> {
            ReplicaRankingContext rankingContext = sms.createRankingContext();
            return rankingContext.indices().size() == 4
                && rankingContext.properties().stream().filter(i -> i.interactiveSize() == 0).toList().isEmpty();
        }));

        setFeatureFlag(true);
        waitUntil(
            () -> cs.state().metadata().index(getDefaultBackingIndexName(dataStream1, 2)).getNumberOfReplicas() == 2
                && cs.state().metadata().index(getDefaultBackingIndexName(dataStream2, 2)).getNumberOfReplicas() == 2,
            2,
            TimeUnit.SECONDS
        );
        for (String datastream : new String[] { dataStream1, dataStream2 }) {
            assertEquals(1, cs.state().metadata().index(getDefaultBackingIndexName(datastream, 1)).getNumberOfReplicas());
            assertEquals(2, cs.state().metadata().index(getDefaultBackingIndexName(datastream, 2)).getNumberOfReplicas());
        }

        // add third data stream, again with 100 docs in first generation, 100 in current write index after rollover
        final String dataStream3 = "logs-es3";
        setupDataStream(dataStream3);
        verifyDocs("logs-es3", 400, 1, 2);
        waitUntil(
            () -> cs.state().metadata().index(getDefaultBackingIndexName(dataStream3, 2)).getNumberOfReplicas() == 2
                && cs.state().metadata().index(getDefaultBackingIndexName(dataStream3, 1)).getNumberOfReplicas() == 2,
            2,
            TimeUnit.SECONDS
        );
        // all write indices should have 2 replicas now
        for (String datastream : new String[] { dataStream1, dataStream2, dataStream3 }) {
            assertEquals(2, cs.state().metadata().index(getDefaultBackingIndexName(datastream, 2)).getNumberOfReplicas());
        }
        // only the youngest (last) backing index (logs-es3) should get two replicas, the other two stay at 1
        assertEquals(1, cs.state().metadata().index(getDefaultBackingIndexName(dataStream1, 1)).getNumberOfReplicas());
        assertEquals(1, cs.state().metadata().index(getDefaultBackingIndexName(dataStream2, 1)).getNumberOfReplicas());
        assertEquals(2, cs.state().metadata().index(getDefaultBackingIndexName(dataStream3, 1)).getNumberOfReplicas());

        // test that data stream with non-interactive data doesn’t get promoted to 2 replicas
        final String dataStream4 = "logs-es4";
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStream4);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());
        indexDocsIntoDatastream(
            dataStream4,
            200,
            System.currentTimeMillis() - DEFAULT_BOOST_WINDOW - 2 * ONE_DAY,
            System.currentTimeMillis() - DEFAULT_BOOST_WINDOW - ONE_DAY
        );
        refresh(dataStream4);
        // write index should stay at 1 replica because it doesn't contain interactive data
        assertEquals(1, cs.state().metadata().index(getDefaultBackingIndexName(dataStream4, 1)).getNumberOfReplicas());

        // add more data to ds4 write index after rolling over, but now inside boost window
        assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStream4, null)).get());
        indexDocsIntoDatastream(dataStream4, 200, System.currentTimeMillis() - DEFAULT_BOOST_WINDOW + ONE_DAY, System.currentTimeMillis());
        refresh(dataStream4);
        // this should scale up ds4 write index, ds3 backing index should get demoted to one replica after some time
        waitUntil(
            () -> cs.state().metadata().index(getDefaultBackingIndexName(dataStream4, 2)).getNumberOfReplicas() == 2
                && cs.state().metadata().index(getDefaultBackingIndexName(dataStream3, 1)).getNumberOfReplicas() == 1,
            2,
            TimeUnit.SECONDS
        );
        for (String datastream : new String[] { dataStream1, dataStream2, dataStream3, dataStream4 }) {
            assertEquals(1, cs.state().metadata().index(getDefaultBackingIndexName(datastream, 1)).getNumberOfReplicas());
            assertEquals(2, cs.state().metadata().index(getDefaultBackingIndexName(datastream, 2)).getNumberOfReplicas());
        }

        // check that adding a regular index regardless of its small size gets it scaled to 2
        var regularIndex = "index1";
        createIndex(regularIndex, indexSettings(1, 1).build());

        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(
            regularIndex,
            10,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(regularIndex);
        waitUntil(() -> cs.state().metadata().index(regularIndex).getNumberOfReplicas() == 2, 2, TimeUnit.SECONDS);
        assertEquals(2, cs.state().metadata().index(regularIndex).getNumberOfReplicas());
    }

    private static void verifyDocs(String dataStream, long expectedNumHits, long minGeneration, long maxGeneration) {
        List<String> expectedIndices = new ArrayList<>();
        for (long k = minGeneration; k <= maxGeneration; k++) {
            expectedIndices.add(getDefaultBackingIndexName(dataStream, k));
        }
        assertResponse(prepareSearch(dataStream).setSize((int) expectedNumHits), resp -> {
            assertThat(resp.getHits().getTotalHits().value(), equalTo(expectedNumHits));
            Arrays.stream(resp.getHits().getHits()).forEach(hit -> assertTrue(expectedIndices.contains(hit.getIndex())));
        });
    }

    private void setupDataStream(String dataStreamName) throws InterruptedException, ExecutionException {
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());

        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocsIntoDatastream(
            dataStreamName,
            200,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).get());
        indexDocsIntoDatastream(
            dataStreamName,
            200,
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(dataStreamName);
    }

    private static void indexDocsIntoDatastream(String dataStream, int numDocs, long minTimestamp, long maxTimestamp) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(randomLongBetween(minTimestamp, maxTimestamp));
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        String backingIndexPrefix = DataStream.BACKING_INDEX_PREFIX + dataStream;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            assertThat(itemResponse.getIndex(), startsWith(backingIndexPrefix));
        }
        indicesAdmin().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    private static void putComposableIndexTemplate(String id, List<String> patterns, @Nullable Settings settings) {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(Template.builder().settings(settings))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

    private static void setFeatureFlag(boolean enabled) throws ExecutionException, InterruptedException {
        assertAcked(
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
                        Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), enabled).build()
                    )
                )
                .get()
        );
    }

    private void indexDocumentsWithTimestamp(String indexName, int numDocs, long minTimestamp, long maxTimestamp) {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(indexName).source(DataStream.TIMESTAMP_FIELD_NAME, randomLongBetween(minTimestamp, maxTimestamp))
            );
        }
        assertNoFailures(bulkRequest.get());
    }
}
