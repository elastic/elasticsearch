/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Correctness of the fast cold activation path of a search shard ({@link FastSearchActivationSettings}): the same hits as the
 * default recovery, no object store discovery requests, new commits still reach the activated shard, and the object store discovery
 * fallback when the indexing shard does not provide a commit.
 */
public class FastSearchActivationIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    /** Counts the object store requests of a node. */
    private static class CountingStrategy extends StatelessMockRepositoryStrategy {
        final AtomicInteger lists = new AtomicInteger();
        final AtomicInteger gets = new AtomicInteger();

        @Override
        public Map<String, BlobContainer> blobContainerChildren(
            CheckedSupplier<Map<String, BlobContainer>, IOException> originalSupplier,
            OperationPurpose purpose
        ) throws IOException {
            lists.incrementAndGet();
            return super.blobContainerChildren(originalSupplier, purpose);
        }

        @Override
        public Map<String, BlobMetadata> blobContainerListBlobs(
            CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
            OperationPurpose purpose
        ) throws IOException {
            lists.incrementAndGet();
            return super.blobContainerListBlobs(originalSupplier, purpose);
        }

        @Override
        public Map<String, BlobMetadata> blobContainerListBlobsByPrefix(
            CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobNamePrefix
        ) throws IOException {
            lists.incrementAndGet();
            return super.blobContainerListBlobsByPrefix(originalSupplier, purpose, blobNamePrefix);
        }

        @Override
        public InputStream blobContainerReadBlob(
            CheckedSupplier<InputStream, IOException> originalSupplier,
            OperationPurpose purpose,
            String blobName,
            long position,
            long length
        ) throws IOException {
            gets.incrementAndGet();
            return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
        }

        void reset() {
            lists.set(0);
            gets.set(0);
        }
    }

    private record Fixture(String indexNode, String searchNode, CountingStrategy searchStore, String indexName, Index index) {}

    /**
     * An index with several flushed commits carrying soft deletes (updates), optionally followed by a refreshed but not yet flushed
     * commit (a virtual batched compound commit that the search shard reads from the indexing shard), and no search copy.
     */
    private Fixture setUpIndex() throws Exception {
        startMasterOnlyNode();
        final String indexNode = startIndexNode();
        final CountingStrategy searchStore = new CountingStrategy();
        final String searchNode = startSearchNode(searchStore);
        ensureStableCluster(3);
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        final int rounds = randomIntBetween(2, 5);
        for (int r = 0; r < rounds; r++) {
            indexDocs(indexName, randomIntBetween(50, 300), () -> Integer.toString(randomIntBetween(0, 600)));
            flush(indexName);
        }
        if (randomBoolean()) {
            indexDocs(indexName, randomIntBetween(1, 50));
            refresh(indexName);
        }
        return new Fixture(indexNode, searchNode, searchStore, indexName, resolveIndex(indexName));
    }

    private static void setFastActivation(boolean enabled) {
        Settings.Builder settings = Settings.builder().put(FastSearchActivationSettings.FAST_ACTIVATION_ENABLED_SETTING.getKey(), enabled);
        if (enabled) {
            settings.put(
                FastSearchActivationSettings.FAST_ACTIVATION_RANGE_SIZE_SETTING.getKey(),
                randomFrom("4kb", "64kb", "128kb", "1mb", "16mb")
            )
                // 0: header/footer warming of every blob; 1gb: every blob fetched whole
                .put(FastSearchActivationSettings.FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING.getKey(), randomFrom("0b", "16kb", "1gb"))
                // step 3: parallel first-query reads
                .put(FastSearchActivationSettings.FAST_ACTIVATION_VECTOR_RANGE_SIZE_SETTING.getKey(), randomFrom("0b", "64kb", "16mb"))
                .put(FastSearchActivationSettings.FAST_ACTIVATION_VECTOR_WHOLE_FILE_MAX_SETTING.getKey(), randomFrom("0b", "1kb", "64mb"))
                .put(SearchService.FETCH_PHASE_PREFETCH_STORED_FIELDS.getKey(), randomBoolean());
        } else {
            settings.putNull(FastSearchActivationSettings.FAST_ACTIVATION_RANGE_SIZE_SETTING.getKey())
                .putNull(FastSearchActivationSettings.FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING.getKey())
                .putNull(FastSearchActivationSettings.FAST_ACTIVATION_VECTOR_RANGE_SIZE_SETTING.getKey())
                .putNull(FastSearchActivationSettings.FAST_ACTIVATION_VECTOR_WHOLE_FILE_MAX_SETTING.getKey())
                .putNull(SearchService.FETCH_PHASE_PREFETCH_STORED_FIELDS.getKey());
        }
        updateClusterSettings(settings);
    }

    /** Activates the search copy from a cold cache and returns a summary of a few queries' hits. */
    private String activateAndQuery(Fixture f) throws Exception {
        f.searchStore.reset();
        setReplicaCount(1, f.indexName);
        ensureGreen(f.indexName);
        return queries(f);
    }

    private void deactivate(Fixture f) throws Exception {
        setReplicaCount(0, f.indexName);
        assertBusy(() -> assertThat(internalCluster().getInstance(IndicesService.class, f.searchNode).indexService(f.index), nullValue()));
        internalCluster().getInstance(PluginsService.class, f.searchNode)
            .filterPlugins(StatelessPlugin.class)
            .findFirst()
            .orElseThrow()
            .getStatelessSharedBlobCacheService()
            .forceEvict(key -> key.shardId().getIndex().equals(f.index));
    }

    private static String queries(Fixture f) {
        StringBuilder sb = new StringBuilder();
        for (var query : new org.elasticsearch.index.query.QueryBuilder[] {
            QueryBuilders.matchAllQuery(),
            QueryBuilders.rangeQuery("field.keyword").gte("m"),
            QueryBuilders.existsQuery("field") }) {
            SearchResponse resp = client(f.searchNode).prepareSearch(f.indexName)
                .setQuery(query)
                .setTrackTotalHits(true)
                .setSize(10_000)
                .addSort("field.keyword", SortOrder.ASC)
                .get();
            try {
                assertNoFailures(resp);
                sb.append(resp.getHits().getTotalHits().value()).append(':');
                List<String> hits = new ArrayList<>();
                for (SearchHit hit : resp.getHits().getHits()) {
                    hits.add(hit.getId() + '=' + hit.getSourceAsMap().get("field"));
                }
                Collections.sort(hits);
                sb.append(hits).append('|');
            } finally {
                resp.decRef();
            }
        }
        return sb.toString();
    }

    public void testSameHitsAsDefaultRecoveryWithoutObjectStoreDiscovery() throws Exception {
        final Fixture f = setUpIndex();

        setFastActivation(false);
        final String expected = activateAndQuery(f);
        assertThat("default recovery discovers the commit in the object store", f.searchStore.lists.get(), greaterThan(0));
        deactivate(f);

        setFastActivation(true);
        final String actual = activateAndQuery(f);
        assertThat(actual, equalTo(expected));
        assertThat("fast activation does not list the object store", f.searchStore.lists.get(), equalTo(0));

        // new commits still reach the fast-activated shard
        indexDocs(f.indexName, 25, () -> "new-" + randomUUID());
        if (randomBoolean()) {
            flush(f.indexName);
        }
        refresh(f.indexName);
        final String afterNewCommit = queries(f);
        assertThat(afterNewCommit, not(equalTo(expected)));

        // the same hits through the default recovery
        deactivate(f);
        setFastActivation(false);
        assertThat(activateAndQuery(f), equalTo(afterNewCommit));
    }

    /**
     * Step 3: vector search and fetch with the vector fill policy (large fills, whole vector files prefetched on the first miss) and
     * the fetch phase prefetching stored fields return the same hits and scores as the default recovery.
     */
    public void testSameKnnAndFetchHitsWithParallelFirstQueryReads() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        final CountingStrategy searchStore = new CountingStrategy();
        final String searchNode = startSearchNode(searchStore);
        ensureStableCluster(3);
        final String indexName = randomIdentifier();
        final String indexType = randomFrom("hnsw", "int8_hnsw", "int8_flat", "bbq_hnsw");
        final int dims = indexType.startsWith("bbq") ? 64 : randomIntBetween(2, 16);
        createIndex(indexName, indexSettings(1, 0).build());
        assertAcked(
            indicesAdmin().preparePutMapping(indexName)
                .setSource(
                    Map.of(
                        "properties",
                        Map.of(
                            "vec",
                            Map.of(
                                "type",
                                "dense_vector",
                                "dims",
                                dims,
                                "similarity",
                                "l2_norm",
                                "index_options",
                                Map.of("type", indexType)
                            )
                        )
                    )
                )
        );
        ensureGreen(indexName);
        final int rounds = randomIntBetween(2, 4);
        int id = 0;
        for (int r = 0; r < rounds; r++) {
            var bulk = client().prepareBulk();
            for (int i = randomIntBetween(50, 300); i > 0; i--) {
                bulk.add(client().prepareIndex(indexName).setId(Integer.toString(id++)).setSource("vec", randomVector(dims), "n", id));
            }
            assertNoFailures(bulk.get());
            flush(indexName);
        }
        final Fixture f = new Fixture(null, searchNode, searchStore, indexName, resolveIndex(indexName));
        final float[] queryVector = randomVector(dims);
        final CheckedSupplier<List<KnnHit>, Exception> knn = () -> {
            setReplicaCount(1, indexName);
            ensureGreen(indexName);
            SearchResponse resp = client(searchNode).prepareSearch(indexName)
                .setKnnSearch(List.of(new KnnSearchBuilder("vec", queryVector, 10, 100, null, null, null)))
                .get();
            try {
                assertNoFailures(resp);
                final List<KnnHit> hits = new ArrayList<>();
                for (SearchHit hit : resp.getHits().getHits()) {
                    hits.add(new KnnHit(hit.getId(), hit.getScore(), hit.getSourceAsMap().get("n")));
                }
                return hits;
            } finally {
                resp.decRef();
            }
        };
        setFastActivation(false);
        final List<KnnHit> expected = knn.get();
        deactivate(f);
        updateClusterSettings(
            Settings.builder()
                .put(FastSearchActivationSettings.FAST_ACTIVATION_ENABLED_SETTING.getKey(), true)
                .put(FastSearchActivationSettings.FAST_ACTIVATION_RANGE_SIZE_SETTING.getKey(), randomFrom("4kb", "128kb", "1mb"))
                .put(FastSearchActivationSettings.FAST_ACTIVATION_VECTOR_RANGE_SIZE_SETTING.getKey(), randomFrom("64kb", "16mb"))
                .put(FastSearchActivationSettings.FAST_ACTIVATION_VECTOR_WHOLE_FILE_MAX_SETTING.getKey(), randomFrom("1kb", "64mb"))
                .put(SearchService.FETCH_PHASE_PREFETCH_STORED_FIELDS.getKey(), true)
        );
        searchStore.reset();
        try {
            assertSameKnnHits(expected, knn.get());
            assertThat(f.searchStore.lists.get(), equalTo(0));
        } finally {
            setFastActivation(false);
        }
    }

    private record KnnHit(String id, float score, Object n) {}

    /**
     * The same hits in the same order, and scores equal up to a tolerance: whether a vector is scored from a direct view of the cache
     * or from a copy depends on what is cached, and the two paths do not always give bit identical scores (the quantized vector types
     * differ in the fourth decimal), which an exact comparison of the scores, even rounded, turns into a flaky failure.
     */
    private static void assertSameKnnHits(List<KnnHit> expected, List<KnnHit> actual) {
        assertThat(actual.stream().map(KnnHit::id).toList(), equalTo(expected.stream().map(KnnHit::id).toList()));
        assertThat(actual.stream().map(KnnHit::n).toList(), equalTo(expected.stream().map(KnnHit::n).toList()));
        for (int i = 0; i < expected.size(); i++) {
            assertThat("score of hit " + expected.get(i).id(), (double) actual.get(i).score(), closeTo(expected.get(i).score(), 1e-3));
        }
    }

    private static float[] randomVector(int dims) {
        float[] v = new float[dims];
        for (int i = 0; i < dims; i++) {
            v[i] = randomFloat();
        }
        return v;
    }

    public void testFallbackWhenRegistrationFails() throws Exception {
        final Fixture f = setUpIndex();
        setFastActivation(false);
        final String expected = activateAndQuery(f);
        deactivate(f);

        setFastActivation(true);
        final AtomicInteger registrations = new AtomicInteger();
        MockTransportService.getInstance(f.indexNode)
            .addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
                if (registrations.incrementAndGet() == 1) {
                    // not retried by TransportSendRecoveryCommitRegistrationAction: the fast path fails and falls back
                    channel.sendResponse(new ElasticsearchException("simulated registration failure"));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });
        try {
            assertThat(activateAndQuery(f), equalTo(expected));
            assertThat(registrations.get(), greaterThanOrEqualTo(2));
            assertThat("fallback discovers the commit in the object store", f.searchStore.lists.get(), greaterThan(0));
        } finally {
            MockTransportService.getInstance(f.indexNode).clearAllRules();
            setFastActivation(false);
        }
    }

    public void testFallbackWhenIndexingShardProvidesWithoutACommit() throws Exception {
        final Fixture f = setUpIndex();
        setFastActivation(false);
        final String expected = activateAndQuery(f);
        deactivate(f);

        setFastActivation(true);
        final AtomicInteger registrations = new AtomicInteger();
        MockTransportService.getInstance(f.indexNode)
            .addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
                if (registrations.incrementAndGet() == 1) {
                    // what an indexing shard without engine answers
                    channel.sendResponse(RegisterCommitResponse.EMPTY);
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });
        try {
            assertThat(activateAndQuery(f), equalTo(expected));
            assertThat(registrations.get(), greaterThanOrEqualTo(2));
            assertThat("fallback discovers the commit in the object store", f.searchStore.lists.get(), greaterThan(0));
        } finally {
            MockTransportService.getInstance(f.indexNode).clearAllRules();
            setFastActivation(false);
        }
    }
}
