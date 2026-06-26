/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING;
import static org.elasticsearch.search.vectors.KnnSearchBuilderTests.randomVector;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

/**
 * End-to-end tests for merge-read abort during index deletion.
 * <p>
 * The latch-based test follows {@code StatelessComponentsOrderIT}: a merge thread blocks in blob-cache
 * {@code openInput} while the index is deleted. Deletion must complete promptly after
 * {@link IndexDirectory#abortMergeReads()} is signalled from the merge scheduler close path.
 * BBQ HNSW timing is additionally covered in {@code IndexEngineTests}.
 */
public class StatelessMergeAbortIT extends AbstractStatelessPluginIntegTestCase {

    private static final String VECTOR_FIELD = "vector";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        return plugins;
    }

    public void testDeleteIndexDuringMergeAbortsReadsQuickly() throws Exception {
        startMasterOnlyNode();
        var nodeSettings = Settings.builder()
            .put(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .build();
        final String indexNode = startIndexNode(nodeSettings);
        ensureStableCluster(2);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final TestStatelessPlugin plugin = findPlugin(indexNode, TestStatelessPlugin.class);
        final var indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        final IndexShard indexShard = findIndexShard(indexName);
        final IndexDirectory indexDirectory = IndexDirectory.unwrapDirectory(indexShard.store().directory());

        for (int i = 0; i < 11; i++) {
            indexDocs(indexName, 10);
            flush(indexName);
        }

        safeAwait(plugin.mergeReadStartedLatch);
        assertThat(indexDirectory.shouldAbortMergeReads(), is(false));

        final long deleteStartNanos = System.nanoTime();
        safeGet(client().admin().indices().prepareDelete(indexName).execute());
        assertThat(System.nanoTime() - deleteStartNanos, lessThan(TimeValue.timeValueSeconds(30).nanos()));
        assertNull(indicesService.indexService(indexShard.shardId().getIndex()));
        assertThat(indexDirectory.shouldAbortMergeReads(), is(true));

        plugin.resumeMergeReadsLatch.countDown();
    }

    public void testDeleteIndexDuringBbqHnswMergeCompletesQuickly() throws Exception {
        final String indexNode = startMasterAndIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        ensureStableCluster(1);

        final String indexName = randomIdentifier();
        final int numDims = randomIntBetween(64, 128);
        createBbqHnswIndex(indexName, numDims);
        ensureGreen(indexName);

        final IndexShard indexShard = findIndexShard(indexName);
        final IndexEngine indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        final IndexDirectory indexDirectory = IndexDirectory.unwrapDirectory(indexShard.store().directory());

        for (int i = 0; i < randomIntBetween(80, 150); i++) {
            indexVectorDocs(indexName, 1, numDims);
            if (i % 10 == 0) {
                assertNoFailures(indicesAdmin().prepareFlush(indexName).execute().get());
            }
        }

        var mergeThread = new Thread(() -> {
            try {
                indexShard.forceMerge(new ForceMergeRequest().maxNumSegments(1).flush(false));
            } catch (Exception e) {
                // merge may be aborted during index deletion
            }
        }, "bbq-hnsw-force-merge");
        mergeThread.start();

        assertBusy(() -> assertTrue("merge was not queued or running before index deletion", indexEngine.hasQueuedOrRunningMerges()));

        final long deleteStartNanos = System.nanoTime();
        safeGet(client().admin().indices().prepareDelete(indexName).execute());
        assertThat(System.nanoTime() - deleteStartNanos, lessThan(TimeValue.timeValueSeconds(30).nanos()));

        assertBusy(() -> assertThat(indexDirectory.shouldAbortMergeReads(), is(true)));

        mergeThread.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse("merge thread should finish after index deletion", mergeThread.isAlive());
    }

    private void createBbqHnswIndex(String indexName, int numDims) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("dims", numDims)
            .field("index", true)
            .field("similarity", "cosine")
            .startObject("index_options")
            .field("type", "bbq_hnsw")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(
                    indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                        .put("index.compound_format", false)
                        .build()
                )
                .setMapping(mapping)
                .get()
        );
    }

    private void indexVectorDocs(String indexName, int numDocs, int numDims) {
        indexDocs(indexName, numDocs, UnaryOperator.identity(), null, () -> Map.of(VECTOR_FIELD, randomVector(numDims)));
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        private final CountDownLatch mergeReadStartedLatch = new CountDownLatch(1);
        private final CountDownLatch resumeMergeReadsLatch = new CountDownLatch(1);

        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexBlobStoreCacheDirectory createIndexBlobStoreCacheDirectory(
            StatelessSharedBlobCacheService cacheService,
            ShardId shardId
        ) {
            return new IndexBlobStoreCacheDirectory(cacheService, shardId) {
                @Override
                protected IndexInput doOpenInput(String name, IOContext context, BlobFileRanges blobFileRanges) {
                    if (ThreadPool.Names.MERGE.equals(EsExecutors.executorName(Thread.currentThread()))) {
                        mergeReadStartedLatch.countDown();
                        try {
                            if (resumeMergeReadsLatch.await(30, TimeUnit.SECONDS) == false) {
                                throw new IllegalStateException("timed out waiting to resume merge reads in test");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("interrupted waiting to resume merge reads in test", e);
                        }
                    }
                    return super.doOpenInput(name, context, blobFileRanges);
                }
            };
        }
    }
}
