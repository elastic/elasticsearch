/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

public class ReaderContextTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        // ReaderContext uses ThreadPool#relativeTimeInMillis; with the default estimated-time cache, short sleeps do not advance time.
        return Settings.builder().put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO).build();
    }

    /**
     * After time exceeds keep-alive since last access, {@link ReaderContext#expiredDueToKeepAlive()} is true.
     */
    public void testExpiredDueToKeepAliveAfterElapsedTime() throws Exception {
        createIndex("index");
        final ReaderContext context = newReaderContext(5L, false);
        try {
            Thread.sleep(15L);
            assertTrue(context.expiredDueToKeepAlive());
        } finally {
            context.close();
        }
    }

    /**
     * Relocating contexts are not treated as keep-alive expiry for {@link ReaderContext#expiredDueToKeepAlive()}.
     */
    public void testExpiredDueToKeepAliveFalseWhileRelocating() throws Exception {
        createIndex("index");
        final ReaderContext context = newReaderContext(1L, false);
        try {
            context.relocate();
            Thread.sleep(10L);
            assertFalse(context.expiredDueToKeepAlive());
        } finally {
            context.close();
        }
    }

    /**
     * While {@link ReaderContext#markAsUsed} holds a ref, keep-alive expiry is suppressed.
     * After release, elapsed time can expire the context.
     * <p>
     * Use {@code markAsUsed(0)} so {@link ReaderContext#markAsUsed} does not raise {@link ReaderContext#keepAlive()}
     * via {@code Math#max}; a larger argument would extend TTL after the releasable closes and this test would need
     * to sleep past that window.
     */
    public void testExpiredDueToKeepAliveFalseWhenMarkAsUsedHeld() throws Exception {
        createIndex("index");
        final ReaderContext context = newReaderContext(1L, false);
        try {
            try (var notUsed = context.markAsUsed(0L)) {
                Thread.sleep(10L);
                assertFalse(context.expiredDueToKeepAlive());
            }
            Thread.sleep(10L);
            assertTrue(context.expiredDueToKeepAlive());
        } finally {
            context.close();
        }
    }

    /**
     * The constructor flag is exposed unchanged by {@link ReaderContext#openedUnderReindexingTask()}.
     */
    public void testOpenedUnderReindexingTask() {
        createIndex("index");
        final ReaderContext reindex = newReaderContext(10L, true);
        try {
            assertTrue(reindex.openedUnderReindexingTask());
        } finally {
            reindex.close();
        }
        final ReaderContext other = newReaderContext(10L, false);
        try {
            assertFalse(other.openedUnderReindexingTask());
        } finally {
            other.close();
        }
    }

    private ReaderContext newReaderContext(long keepAliveMillis, boolean openedUnderReindexingTask) {
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(resolveIndex("index"));
        final IndexShard indexShard = indexService.getShard(0);
        return new ReaderContext(
            new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong()),
            indexService,
            indexShard,
            indexShard.acquireSearcherSupplier(),
            keepAliveMillis,
            randomBoolean(),
            openedUnderReindexingTask
        );
    }
}
