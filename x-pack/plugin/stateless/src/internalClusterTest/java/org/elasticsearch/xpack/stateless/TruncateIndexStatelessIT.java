/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.admin.indices.truncate.TruncateIndexAction;
import org.elasticsearch.action.admin.indices.truncate.TruncateIndexRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.engine.HollowIndexEngine;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Truncate in stateless: search shards do not take part in replication, so they have to follow the indexing shard through the commit
 * it produces, and a hollow indexing shard has to be unhollowed before it can be truncated.
 */
public class TruncateIndexStatelessIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    private static BroadcastResponse truncate(String index) {
        return client().execute(TruncateIndexAction.INSTANCE, new TruncateIndexRequest(index)).actionGet();
    }

    /**
     * A search shard has to serve the truncated state as soon as the truncate is acknowledged, without anything else refreshing it.
     */
    public void testSearchShardSeesTheTruncate() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final int docsBefore = between(20, 50);
        indexDocsAndRefresh(indexName, docsBefore);
        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), docsBefore);

        assertAllSuccessful(truncate(indexName));

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), 0L);

        // and what is indexed afterwards is what the search shard serves
        final int docsAfter = between(5, 15);
        indexDocsAndRefresh(indexName, docsAfter);
        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), docsAfter);
    }

    /**
     * A shard that was hollowed because it had been idle must be unhollowed by a truncate, the same way a write would.
     */
    public void testTruncateUnhollowsAHollowShard() throws Exception {
        final String indexNodeA = startMasterAndIndexNode(
            Settings.builder()
                .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
                .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(1))
                .build()
        );
        final String indexNodeB = startMasterAndIndexNode(
            Settings.builder()
                // just to ensure that the shard is not hollowed on this node
                .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueDays(1))
                .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(1))
                .build()
        );

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);
        indexDocs(indexName, between(20, 50));
        flush(indexName);

        // let the shard become hollow by relocating it
        final var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexNodeA);
        assertBusy(() -> assertTrue(hollowShardsService.isHollowableIndexShard(findIndexShard(indexName))));
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        ensureGreen(indexName);
        assertThat(findIndexShard(resolveIndex(indexName), 0, indexNodeB).getEngineOrNull(), instanceOf(HollowIndexEngine.class));

        assertAllSuccessful(truncate(indexName));

        assertThat(findIndexShard(resolveIndex(indexName), 0, indexNodeB).getEngineOrNull(), instanceOf(IndexEngine.class));
        final long docs = client().admin()
            .indices()
            .prepareStats(indexName)
            .clear()
            .setDocs(true)
            .get()
            .getPrimaries()
            .getDocs()
            .getCount();
        assertThat(docs, equalTo(0L));
    }
}
