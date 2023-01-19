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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.action.NewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

public class StatelessSearchIT extends AbstractStatelessIntegTestCase {

    /**
     * A testing stateless plugin that extends the {@link Engine.IndexCommitListener} to count created number of commits.
     */
    public static class TestStateless extends Stateless {

        private final AtomicInteger createdCommits = new AtomicInteger(0);

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected Engine.IndexCommitListener createIndexCommitListener() {
            Engine.IndexCommitListener superListener = super.createIndexCommitListener();
            return new Engine.IndexCommitListener() {

                @Override
                public void onNewCommit(
                    ShardId shardId,
                    Store store,
                    long primaryTerm,
                    Engine.IndexCommitRef indexCommitRef,
                    Set<String> additionalFiles
                ) {
                    createdCommits.incrementAndGet();
                    superListener.onNewCommit(shardId, store, primaryTerm, indexCommitRef, additionalFiles);
                }

                @Override
                public void onIndexCommitDelete(ShardId shardId, IndexCommit deletedCommit) {
                    superListener.onIndexCommitDelete(shardId, deletedCommit);
                }
            };
        }

        private int getCreatedCommits() {
            return createdCommits.get();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class, TestStateless.class);
    }

    private static int getNumberOfCreatedCommits() {
        int numberOfCreatedCommits = 0;
        for (String node : internalCluster().getNodeNames()) {
            var plugin = internalCluster().getInstance(PluginsService.class, node).filterPlugins(TestStateless.class).get(0);
            numberOfCreatedCommits += plugin.getCreatedCommits();
        }
        return numberOfCreatedCommits;
    }

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

    public void testSearchShardsNotifiedOnNewCommits() throws Exception {
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
        startSearchNodes(numReplicas);
        updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas));
        ensureGreen(indexName);

        final AtomicInteger searchNotifications = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addRequestHandlingBehavior(NewCommitNotificationAction.NAME, (handler, request, channel, task) -> {
                if (request instanceof NewCommitNotificationRequest commitRequest) {
                    if (commitRequest.isIndexingShard() == false) {
                        searchNotifications.incrementAndGet();
                    }
                }
                handler.messageReceived(request, channel, task);
            });
        }

        final int beginningNumberOfCreatedCommits = getNumberOfCreatedCommits();

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            switch (randomInt(2)) {
                case 0 -> client().admin().indices().prepareFlush().setForce(randomBoolean()).get();
                case 1 -> client().admin().indices().prepareRefresh().get();
                case 2 -> client().admin().indices().prepareForceMerge().get();
            }
        }

        assertBusy(() -> {
            assertThat(
                "Search shard notifications should be equal to the number of created commits multiplied by the number of replicas.",
                searchNotifications.get(),
                equalTo((getNumberOfCreatedCommits() - beginningNumberOfCreatedCommits) * numReplicas)
            );
        });
    }

    public void testRefresh() throws Exception {
        startIndexNode();
        startSearchNodes(numShards * numReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                .build()
        );
        ensureGreen(indexName);
        int docsToIndex = randomIntBetween(1, 100);
        indexDocs(indexName, docsToIndex);
        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());
        var searchResponse = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
        assertNoFailures(searchResponse);
        assertEquals(docsToIndex, searchResponse.getHits().getTotalHits().value);
    }

}
