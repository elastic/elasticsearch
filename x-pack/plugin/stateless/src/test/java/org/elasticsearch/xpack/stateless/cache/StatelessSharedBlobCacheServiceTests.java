/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils;
import org.elasticsearch.xpack.stateless.cache.reader.ObjectStoreCacheBlobReader;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThan;

public class StatelessSharedBlobCacheServiceTests extends ESTestCase {

    private final int regionPages = 10;
    private final int blobLength = regionPages * SharedBytes.PAGE_SIZE;
    private final ByteSizeValue regionSize = ByteSizeValue.ofBytes(blobLength);

    public void testAsyncPrefetchCallsListenerOnIndexingNode() throws Exception {
        Settings nodeSettings = createSettings("index", regionSize);
        BlobCacheMetrics metrics = new BlobCacheMetrics(MeterRegistry.NOOP);

        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(nodeSettings, TestEnvironment.newEnvironment(nodeSettings));
            TestThreadPool testThreadPool = createThreadPool()
        ) {
            StatelessSharedBlobCacheService service = new StatelessSharedBlobCacheService(
                nodeEnvironment,
                nodeSettings,
                testThreadPool,
                metrics,
                System::currentTimeMillis,
                null
            );
            CountDownLatch latch = new CountDownLatch(1);
            ActionListener<Void> listener = ActionListener.wrap(r -> latch.countDown(), e -> {});
            service.asyncPrefetch(null, 10, 0, 10, null, listener);

            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
    }

    public void testAsyncPrefetchPopulatesCacheOnSearchNode() throws Exception {
        Settings nodeSettings = createSettings("search", regionSize);
        BlobCacheMetrics metrics = new BlobCacheMetrics(MeterRegistry.NOOP);
        String fileName = "test-file";
        byte[] blobContent = randomByteArrayOfLength(blobLength);

        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(nodeSettings, TestEnvironment.newEnvironment(nodeSettings));
            TestThreadPool testThreadPool = createThreadPool()
        ) {
            StatelessSharedBlobCacheService service = new StatelessSharedBlobCacheService(
                nodeEnvironment,
                nodeSettings,
                testThreadPool,
                metrics,
                System::currentTimeMillis,
                null
            );

            FileCacheKey cacheKey = new FileCacheKey(new ShardId(new Index("idx", "uid"), 0), 1L, fileName);
            AtomicInteger counter = new AtomicInteger(0);
            ObjectStoreCacheBlobReader cacheBlobReader = new ObjectStoreCacheBlobReader(
                TestUtils.singleBlobContainer(fileName, blobContent),
                fileName,
                service.getRangeSize(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            ) {
                @Override
                public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                    counter.incrementAndGet();
                    super.getRangeInputStream(position, length, listener);
                }
            };

            CountDownLatch latch = new CountDownLatch(1);
            ActionListener<Void> listener = ActionListener.wrap(r -> latch.countDown(), e -> {});
            service.asyncPrefetch(cacheKey, blobLength, 0, blobLength, cacheBlobReader, listener);

            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertThat(counter.get(), greaterThan(0));
        }
    }

    private Settings createSettings(String nodeRole, ByteSizeValue regionSize) {
        return Settings.builder()
            .putList("node.roles", nodeRole)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .put(
                SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes(50 * SharedBytes.PAGE_SIZE).getStringRep()
            )
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), regionSize.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), regionSize.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .build();
    }
}
