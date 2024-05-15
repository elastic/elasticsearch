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

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class AutoscalingReplicaIT extends AbstractStatelessIntegTestCase {

    private static final long DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(7).millis();
    private static final long ONE_DAY = TimeValue.timeValueDays(1).millis();

    public void testSearchPowerAffectsReplica() throws Exception {
        Settings settings = Settings.builder()
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
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
                    new ClusterUpdateSettingsRequest().persistentSettings(
                        Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), searchPowerOver250).build()
                    )
                )
                .get()
        );
        waitUntil(() -> { return clusterService.state().metadata().index(indexName).getNumberOfReplicas() == 2; }, 5, TimeUnit.SECONDS);
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
        waitUntil(() -> { return clusterService.state().metadata().index(indexName2).getNumberOfReplicas() == 2; }, 5, TimeUnit.SECONDS);
        assertEquals(2, clusterService.state().metadata().index(indexName2).getNumberOfReplicas());

        // back to SP 100
        assertAcked(
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest().persistentSettings(
                        Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 100).build()
                    )
                )
                .get()
        );
        waitUntil(() -> { return clusterService.state().metadata().index(indexName).getNumberOfReplicas() == 1; }, 5, TimeUnit.SECONDS);
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
                    new ClusterUpdateSettingsRequest().persistentSettings(
                        Settings.builder()
                            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 220)
                            .put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), true)
                            .build()
                    )
                )
                .get()
        );
        // scaling up should happen almost immediately
        waitUntil(() -> { return clusterService.state().metadata().index(index1).getNumberOfReplicas() == 2; }, 1, TimeUnit.SECONDS);
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
        waitUntil(() -> { return clusterService.state().metadata().index(index2).getNumberOfReplicas() == 2; }, 1, TimeUnit.SECONDS);
        assertEquals(2, clusterService.state().metadata().index(index2).getNumberOfReplicas());
        // index1 should still have 2 replicas, it needs 6*500ms for the change to stabiliza
        assertEquals(2, clusterService.state().metadata().index(index1).getNumberOfReplicas());

        waitUntil(() -> { return clusterService.state().metadata().index(index1).getNumberOfReplicas() == 1; }, 4, TimeUnit.SECONDS);
        assertEquals(1, clusterService.state().metadata().index(index1).getNumberOfReplicas());
    }

    public void testDisablingReplicasScalesDown() throws Exception {
        // start in a state with an index scaled to two replicas
        Settings settings = Settings.builder()
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
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
                    new ClusterUpdateSettingsRequest().persistentSettings(
                        Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), 250).build()
                    )
                )
                .get()
        );
        waitUntil(() -> { return clusterService.state().metadata().index(indexName).getNumberOfReplicas() == 2; }, 2, TimeUnit.SECONDS);
        assertEquals(2, clusterService.state().metadata().index(indexName).getNumberOfReplicas());

        // now disable feature
        assertAcked(
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest().persistentSettings(
                        Settings.builder().put(ServerlessSharedSettings.ENABLE_REPLICAS_FOR_INSTANT_FAILOVER.getKey(), false).build()
                    )
                )
                .get()
        );
        waitUntil(() -> { return clusterService.state().metadata().index(indexName).getNumberOfReplicas() == 1; }, 2, TimeUnit.SECONDS);
        assertEquals(1, clusterService.state().metadata().index(indexName).getNumberOfReplicas());
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
