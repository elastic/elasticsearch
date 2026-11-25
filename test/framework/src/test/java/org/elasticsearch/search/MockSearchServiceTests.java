/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockSearchServiceTests extends ESTestCase {

    /**
     * A leaked context has to identify itself, because these leaks are rare races that are not reproducible
     * locally and are usually only ever seen once, in a CI log.
     */
    public void testAssertNoInFlightContext() {
        final ReaderContext reader = stubbedReaderContext(7L);
        MockSearchService.addActiveContext(reader);
        try {
            Throwable e = expectThrows(AssertionError.class, () -> MockSearchService.assertNoInFlightContext());
            assertThat(
                e.getMessage(),
                startsWith(
                    "There are still [1] in-flight contexts. The first one's creation site is listed as the cause of this exception."
                )
            );
            assertThat(e.getMessage(), containsString("on shard [test-index][0] of node test-node"));
            assertThat(e.getMessage(), containsString("id=[test-session][42]"));
            assertThat(e.getMessage(), containsString("creatorTask=7"));
            assertThat(e.getMessage(), containsString("singleSession=false"));
            assertThat(e.getMessage(), containsString("keepAlive=5m"));
            assertThat(e.getMessage(), containsString("held for"));

            // relocated point in time contexts do not carry the id of the task that opened them
            when(reader.creatorTaskId()).thenReturn(0L);
            assertThat(
                expectThrows(AssertionError.class, () -> MockSearchService.assertNoInFlightContext()).getMessage(),
                containsString("creatorTask=unknown")
            );

            e = e.getCause();
            assertEquals(MockSearchService.class.getName(), e.getStackTrace()[0].getClassName());
            assertEquals(MockSearchServiceTests.class.getName(), e.getStackTrace()[1].getClassName());
        } finally {
            MockSearchService.removeActiveContext(reader);
        }
    }

    /**
     * A context can be closed while it is being described, so describing it is best effort. The leak still has to
     * be reported, otherwise a failure to read the diagnostics would hide the leak that the assertion exists to find.
     */
    public void testAssertNoInFlightContextWhenDetailsAreUnavailable() {
        final ReaderContext reader = mock(ReaderContext.class);
        MockSearchService.addActiveContext(reader);
        try {
            final AssertionError e = expectThrows(AssertionError.class, () -> MockSearchService.assertNoInFlightContext());
            assertThat(e.getMessage(), startsWith("There are still [1] in-flight contexts."));
            assertThat(e.getMessage(), containsString("details unavailable"));
        } finally {
            MockSearchService.removeActiveContext(reader);
        }
    }

    /**
     * A real ReaderContext needs a started IndexShard and an open searcher, which is far more setup than a test
     * covering how a leak is reported needs, so only the fields that reach the message are stubbed.
     */
    private static ReaderContext stubbedReaderContext(long creatorTaskId) {
        final ShardId shardId = new ShardId("test-index", "_na_", 0);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.routingEntry()).thenReturn(TestShardRouting.newShardRouting(shardId, "test-node", true, ShardRoutingState.STARTED));
        final ReaderContext reader = mock(ReaderContext.class);
        when(reader.indexShard()).thenReturn(indexShard);
        when(reader.id()).thenReturn(new ShardSearchContextId("test-session", 42L));
        when(reader.creatorTaskId()).thenReturn(creatorTaskId);
        when(reader.singleSession()).thenReturn(false);
        when(reader.keepAlive()).thenReturn(TimeValue.timeValueMinutes(5).millis());
        return reader;
    }
}
