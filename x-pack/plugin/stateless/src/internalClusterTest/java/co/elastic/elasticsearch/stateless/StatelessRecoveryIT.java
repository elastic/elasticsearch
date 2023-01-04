package co.elastic.elasticsearch.stateless;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class StatelessRecoveryIT extends AbstractStatelessIntegTestCase {

    private final int numShards = randomIntBetween(1, 3);
    private final int numReplicas = randomIntBetween(1, 2);

    @Before
    public void init() {
        startMasterOnlyNode();
        startIndexNodes(numShards);
    }

    public void testSearchShardsStarted() {
        startSearchNodes(numShards * numReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);
    }

    public void testSearchShardsStartedAfterIndexShards() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        startSearchNodes(numShards * numReplicas);
        updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        ensureGreen(indexName);
    }

    public void testSearchShardsStartedWithDocs() {
        startSearchNodes(numShards * numReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 100));
        if (randomBoolean()) {
            var flushResponse = flush(indexName);
            assertEquals(RestStatus.OK, flushResponse.getStatus());
        }
        ensureGreen(indexName);
    }

    public void testSearchShardsStartedAfterIndexShardsWithDocs() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 100));
        if (randomBoolean()) {
            var flushResponse = flush(indexName);
            assertEquals(RestStatus.OK, flushResponse.getStatus());
        }

        startSearchNodes(numShards * numReplicas);
        updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas));
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 100));
        if (randomBoolean()) {
            var flushResponse = flush(indexName);
            assertEquals(RestStatus.OK, flushResponse.getStatus());
        }
        ensureGreen(indexName);
    }

    public void testRelocatingIndexShards() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 100));
        if (randomBoolean()) {
            flush(indexName);
        }

        final int iters = randomIntBetween(5, 10);
        for (int i = 0; i < iters; i++) {
            final AtomicBoolean running = new AtomicBoolean(true);

            final Thread[] threads = new Thread[scaledRandomIntBetween(1, 3)];
            for (int j = 0; j < threads.length; j++) {
                threads[j] = new Thread(() -> {
                    while (running.get()) {
                        indexDocs(indexName, 20);
                    }
                });
                threads[j].start();
            }
            final Set<String> existingNodes = new HashSet<>(internalCluster().nodesInclude(indexName));
            if (existingNodes.size() == 1) {
                startIndexNode();
            }

            final String stoppedNode = randomFrom(existingNodes);
            updateIndexSettings(indexName, Settings.builder().put("index.routing.allocation.exclude._name", stoppedNode));
            ensureGreen(TimeValue.timeValueSeconds(30L), indexName);
            running.set(false);
            for (Thread thread : threads) {
                thread.join();
            }
            internalCluster().stopNode(stoppedNode);
            ensureGreen(indexName);
        }
    }
}
