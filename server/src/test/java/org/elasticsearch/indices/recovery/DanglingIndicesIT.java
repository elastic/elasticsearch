package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.After;

import java.util.Collections;

import static org.elasticsearch.cluster.metadata.IndexGraveyard.SETTING_MAX_TOMBSTONES;
import static org.elasticsearch.gateway.DanglingIndicesState.ALLOCATE_DANGLING_INDICES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(numDataNodes = 3)
public class DanglingIndicesIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test-idx-1";

    private static final int MIN_DOC_COUNT = 500;
    private static final int MAX_DOC_COUNT = 1000;
    private static final int SHARD_COUNT = 1;
    private static final int REPLICA_COUNT = 2;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Don't keep any indices in the graveyard, so that when we delete an index,
            // it's definitely considered to be dangling.
            .put(SETTING_MAX_TOMBSTONES.getKey(), 0)
            .build();
    }

    @After
    public void cleanup() {
        // Set to null in order to clean up whatever actions the tests took
        setRecoveryEnabled(null);
    }

    /**
     * Check that when dangling indices are discovered, then they are recovered into
     * the cluster, so long as the recovery setting is enabled.
     */
    public void testDanglingIndicesAreRecoveredWhenSettingIsEnabled() throws Exception {
        logger.info("--> starting cluster");
        internalCluster().startNodes();

        // Create an index and distribute it across the 3 nodes
        createAndPopulateIndex(INDEX_NAME, SHARD_COUNT, REPLICA_COUNT);
        ensureGreen();

        // Recover dangling indices automatically.
        setRecoveryEnabled(true);

        // This is so that when then node comes back up, we have a dangling index that can be recovered.
        logger.info("--> restarted a random node and deleting the index while it's down");
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                deleteIndex(INDEX_NAME);
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen();

        assertTrue("Expected dangling index to be recovered", indexExists(INDEX_NAME));
    }

    /**
     * Check that when dangling indices are discovered, then they are not recovered into
     * the cluster when the recovery setting is disabled.
     */
    public void testDanglingIndicesAreNotRecoveredWhenSettingIsDisabled() throws Exception {
        logger.info("--> starting cluster");
        internalCluster().startNodes();

        // Create an index and distribute it across the 3 nodes
        createAndPopulateIndex(INDEX_NAME, SHARD_COUNT, REPLICA_COUNT);

        // Create another index so that once we drop the first index, we
        // can still assert that the cluster is green.
        createAndPopulateIndex(INDEX_NAME + "-other", SHARD_COUNT, REPLICA_COUNT);

        ensureGreen();

        // This is so that when then node comes back up, we have a dangling index that could
        // be recovered, but shouldn't be.
        logger.info("--> restarted a random node and deleting the index while it's down");
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                deleteIndex(INDEX_NAME);
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen();

        assertFalse("Expected dangling index to be recovered", indexExists(INDEX_NAME));
    }

    private void createAndPopulateIndex(String name, int shardCount, int replicaCount) throws InterruptedException {
        logger.info("--> creating test index: {}", name);
        assertAcked(
            prepareCreate(
                name,
                Settings.builder()
                    .put("number_of_shards", shardCount)
                    .put("number_of_replicas", replicaCount)
                    .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), 0)
            )
        );
        ensureGreen();

        logger.info("--> indexing sample data");
        final int numDocs = between(MIN_DOC_COUNT, MAX_DOC_COUNT);
        final IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];

        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex(name)
                .setSource("foo-int", randomInt(), "foo-string", randomAlphaOfLength(32), "foo-float", randomFloat());
        }

        indexRandom(true, docs);
        flush();
        assertThat(client().prepareSearch(name).setSize(0).get().getHits().getTotalHits().value, equalTo((long) numDocs));

        client().admin().indices().prepareStats(name).execute().actionGet();
    }

    private void deleteIndex(String indexName) {
        logger.info("--> deleting test index: {}", indexName);

        assertAcked(client().admin().indices().prepareDelete(indexName));
    }

    private void setRecoveryEnabled(Boolean enabled) {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                // Map.of doesn't like null ¯\_(ツ)_/¯
                .setTransientSettings(Collections.singletonMap(ALLOCATE_DANGLING_INDICES_SETTING.getKey(), enabled))
        );
    }
}
