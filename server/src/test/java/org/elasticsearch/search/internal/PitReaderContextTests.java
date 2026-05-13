/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.cluster.routing.SplitShardCountSummary;
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
                // This allows to have predictable time that is useful for tests below.
                // Time will tick every time PitReaderContext#nowInMillis() is called,
                // meaning in constructor, during isExpired() and when releasing
                // a releasable obtained from markAsUsed.
                return time.getAndUpdate(t -> t + 1000);
            }
        };
        when(shard.getThreadPool()).thenReturn(threadPool);

        try (threadPool) {
            // lastAccessTime here is 0 with our time setup above.
            var context = new PitReaderContext(
                new ShardSearchContextId("session", 0),
                null,
                shard,
                null,
                100,
                null,
                SplitShardCountSummary.IRRELEVANT
            );

            // Calling isExpired() ticks a timer and time is now 1000
            // which is larger than keepAlive so the context is expired.
            assertTrue(context.isExpired());

            var releasable = context.markAsUsed(1100);
            // One outstanding user, keepAlive doesn't matter.
            assertFalse(context.isExpired());

            // Current time is 2000 and lastAccessTime is set to 2000.
            releasable.close();
            // Calling isExpired() ticks a timer and time is now 3000.
            // There is no outstanding users but since keepAlive was set to 1100 above, we are within the keepAlive
            // (3000 - 2000 = 1000) so the context is not expired.
            assertFalse(context.isExpired());

            var releasable2 = context.markAsUsed(100_000);
            // In use again;
            assertFalse(context.isExpired());

            // Current time is 4000 and relocatedTimestampMs is set to 4000.
            context.relocate();
            // Calling isExpired() ticks a timer and time is now 5000.
            // The context is relocating, but we are within the relocation grace period (it's exactly 1000 ms so one tick is covered by it).
            assertFalse(context.isExpired());
            // Calling isExpired() ticks a timer and time is now 6000.
            // Relocation grace period has elapsed ((6000 - 4000 = 2000) > 1000)
            // but there is one outstanding user so we can't expire.
            assertFalse(context.isExpired());

            // Current time is 7000 and lastAccessTime is set to 7000.
            releasable2.close();
            // Once there is no users and after the grace period (it elapsed during last check)
            // we are expired even though we didn't reach the very large keepAlive we've set.
            assertTrue(context.isExpired());
        }
    }
}
