/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.parquet.repository.ParquetBlobCache;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Does a mount that has a node-local cache read each byte range of its file once? The first search fills the cache from the file, and
 * a second search that reads the same column chunks must not write to the cache again.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ParquetCacheIT extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        // only one engine factory may apply to an index, and ours must be the one
        return false;
    }

    @Override
    protected boolean forceSingleDataPath() {
        // the cache is not used on a node with several data paths
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ParquetDocValuesPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ParquetBlobCache.CACHE_SIZE.getKey(), "32mb")
            .put(ParquetBlobCache.CACHE_REGION_SIZE.getKey(), "1mb")
            .build();
    }

    public void testASecondSearchReadsFromTheCache() throws Exception {
        internalCluster().startNode();
        ParquetMounts.writeFileAndRegisterRepository();
        ParquetMounts.mount(this);
        ParquetBlobCache cache = internalCluster().getInstance(ParquetBlobCache.class);

        ParquetMounts.assertServesTheFile();
        SharedBlobCacheService.Stats afterFirst = cache.stats();
        assertThat("the first search must fill the cache", afterFirst.writeBytes(), greaterThan(0L));

        // our file has its own name, so that it cannot clash with the cache of searchable snapshots (which deletes its file at a size of 0)
        Path dataPath = internalCluster().getInstance(NodeEnvironment.class).nodeDataPaths()[0];
        assertTrue(Files.exists(dataPath.resolve("parquet_cache")));
        assertFalse(Files.exists(dataPath.resolve("shared_snapshot_cache")));

        ParquetMounts.assertServesTheFile();
        SharedBlobCacheService.Stats afterSecond = cache.stats();
        assertEquals("the second search must not write to the cache", afterFirst.writeBytes(), afterSecond.writeBytes());
        assertEquals("the second search must not miss", afterFirst.missCount(), afterSecond.missCount());
    }
}
