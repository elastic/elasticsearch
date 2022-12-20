package co.elastic.elasticsearch.stateless;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;

import java.util.Locale;

public class StatelessSearchIT extends AbstractStatelessIntegTestCase {

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
}
