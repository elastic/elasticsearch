/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;

abstract class AbstractBlobCacheMetricsIntegTestCase extends AbstractStatelessPluginIntegTestCase {

    protected final String createIndexWithNoReplicas(String namePrefix) {
        final String indexName = namePrefix + "-" + randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
            )
        );
        return indexName;
    }

    /**
     * Use at least 2 segments so that index data extends beyond cache region 0. ShardWarmer skips
     * region 0 for SEARCH warming (it's assumed already loaded), so with a single small segment all
     * file locations land in region 0 and warming records no metrics.
     */
    protected final void populateIndex(String indexName) {
        final int iters = randomIntBetween(2, 3);
        int docsCounter = 0;
        for (int i = 0; i < iters; i++) {
            int numDocs = randomIntBetween(100, 1_000);
            indexDocs(indexName, numDocs);
            refresh(indexName);
            docsCounter += numDocs;
        }
        logger.info("--> Wrote {} documents in {} segments to index {}", docsCounter, iters, indexName);
    }

    protected final void clearShardCache(IndexShard indexShard) {
        BlobStoreCacheDirectory blobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(indexShard.store().directory());
        getCacheService(blobStoreCacheDirectory).forceEvict(key -> true);
    }
}
