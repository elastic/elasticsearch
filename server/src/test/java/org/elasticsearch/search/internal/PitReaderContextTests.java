/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PitReaderContextTests extends ESTestCase {
    public void testIsExpired() {
        var time = new AtomicLong(0);

        var shard = mock(IndexShard.class);
        var threadPool = new TestThreadPool("test") {
            @Override
            public long relativeTimeInMillis() {
                return time.getAndUpdate(t -> t + 1000);
            }
        };
        when(shard.getThreadPool()).thenReturn(threadPool);

        try (threadPool) {
            var context = new PitReaderContext(new ShardSearchContextId("session", 0), null, shard, null, 100);

            // No outstanding users and timer tick is larger than keep alive
            // (1000 millis has passed) so it's expired.
            assertTrue(context.isExpired());

            var releasable = context.markAsUsed(1100);
            // One outstanding user.
            assertFalse(context.isExpired());

            releasable.close();
            // No outstanding users but the keepalive is now larger than the tick and so it is not expired due to last access time
            // (1000 millis passed but keep alive is 1100);
            assertFalse(context.isExpired());

            var releasable2 = context.markAsUsed(100_000);
            // Is use again;
            assertFalse(context.isExpired());

            context.relocate();
            // We relocated, but we are within the relocation grace period (it's exactly 1000 ms so one tick is covered by it).
            assertFalse(context.isExpired());
            // But even after the grace period there is one outstanding user so we can't expire.
            assertFalse(context.isExpired());

            releasable2.close();
            // Once there is no users and after the grace period (it elapsed during last check)
            // we are expired even though we didn't reach keepalive.
            assertTrue(context.isExpired());
        }
    }
}
