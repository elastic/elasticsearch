/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyList;
import static org.elasticsearch.core.TimeValue.ZERO;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.is;

/**
 * Tests {@link SearchContextKeepaliveDeadline}: inferred keep-alive expiry window from successful search extensions,
 * accumulation of the latest deadline across requests, and when {@link SearchContextKeepaliveDeadline#shouldRecordKeepaliveExpiry}
 * advises recording the keep-alive-expired heuristic.
 */
public class SearchContextKeepaliveDeadlineTests extends ESTestCase {

    private static SearchContextMissingException missingContext() {
        return new SearchContextMissingException(new ShardSearchContextId("session", 1L));
    }

    /** Before any extension, the deadline is unset so past-deadline checks and expiry recording stay disabled. */
    public void testInitiallyNoExpiryGuess() {
        AtomicLong clock = new AtomicLong(5_000L);
        SearchContextKeepaliveDeadline d = new SearchContextKeepaliveDeadline(clock::get);
        assertThat(d.isPastKeepaliveDeadline(), is(false));
        assertThat(d.shouldRecordKeepaliveExpiry(missingContext(), emptyList()), is(false));
    }

    /**
     * One successful extension shifts {@link SearchContextKeepaliveDeadline#isPastKeepaliveDeadline}
     * after clock passes {@code now + keepAlive}.
     * */
    public void testSuccessfulExtensionAdvancesDeadline() {
        AtomicLong clock = new AtomicLong(10_000L);
        SearchContextKeepaliveDeadline d = new SearchContextKeepaliveDeadline(clock::get);
        d.recordSuccessfulExtension(timeValueMillis(50)); // deadline 10050
        clock.set(10_049); // still at or before inferred expiry
        assertThat(d.isPastKeepaliveDeadline(), is(false));
        clock.set(10_051);
        assertThat(d.isPastKeepaliveDeadline(), is(true));
    }

    /** Past-deadline uses strict ordering: equal to {@code now + keepAlive} is not yet past. */
    public void testPastDeadlineUsesStrictGreaterThan() {
        AtomicLong clock = new AtomicLong(10_000L);
        SearchContextKeepaliveDeadline d = new SearchContextKeepaliveDeadline(clock::get);
        d.recordSuccessfulExtension(timeValueMillis(50)); // deadline 10050
        clock.set(10_050);
        assertThat(d.isPastKeepaliveDeadline(), is(false));
        clock.set(10_051);
        assertThat(d.isPastKeepaliveDeadline(), is(true));
    }

    /** Zero keep-alive still anchors the deadline at {@code now} (no extension slack). */
    public void testZeroKeepAliveAnchorsDeadlineAtNow() {
        AtomicLong clock = new AtomicLong(5L);
        SearchContextKeepaliveDeadline d = new SearchContextKeepaliveDeadline(clock::get);
        d.recordSuccessfulExtension(ZERO);
        clock.set(5L);
        assertThat(d.isPastKeepaliveDeadline(), is(false));
        clock.set(6L);
        assertThat(d.isPastKeepaliveDeadline(), is(true));
    }

    /** A later successful search can push the inferred deadline beyond an earlier batch's window. */
    public void testLaterExtensionCanGrowDeadlineBeyondEarlierWindow() {
        AtomicLong clock = new AtomicLong(0L);
        SearchContextKeepaliveDeadline d = new SearchContextKeepaliveDeadline(clock::get);
        d.recordSuccessfulExtension(timeValueMillis(50)); // deadline 50
        clock.set(40L);
        d.recordSuccessfulExtension(timeValueMillis(200)); // candidate 240, max(50, 240)
        clock.set(239L);
        assertThat(d.isPastKeepaliveDeadline(), is(false));
        clock.set(241L);
        assertThat(d.isPastKeepaliveDeadline(), is(true));
    }

    /** Repeated extensions use {@code max} of projected deadlines so a shorter later extension cannot shrink the window. */
    public void testAccumulateUsesMaxDeadline() {
        AtomicLong clock = new AtomicLong(1_000L);
        SearchContextKeepaliveDeadline d = new SearchContextKeepaliveDeadline(clock::get);
        d.recordSuccessfulExtension(timeValueMillis(100)); // deadline 1100
        clock.set(2_000L);
        d.recordSuccessfulExtension(timeValueMillis(50)); // now + 50 -> 2050, max(2050,1100)=2050
        clock.set(2_049);
        assertThat(d.isPastKeepaliveDeadline(), is(false));
        clock.set(2_051);
        assertThat(d.isPastKeepaliveDeadline(), is(true));
    }

    /**
     * Records only when past the inferred deadline and failure carries {@link SearchContextMissingException}
     * (top-level or in shard failures).
     * */
    public void testRecordsOnlyWhenPastDeadlineAndMissingContext() {
        AtomicLong clock = new AtomicLong(0L);
        SearchContextKeepaliveDeadline d = new SearchContextKeepaliveDeadline(clock::get);
        d.recordSuccessfulExtension(timeValueMillis(10));
        clock.set(100L);

        Exception unrelated = new IllegalStateException("foo");
        assertThat(d.shouldRecordKeepaliveExpiry(unrelated, emptyList()), is(false));

        List<PaginatedSearchFailure> shardFailures = List.of(new PaginatedSearchFailure(missingContext(), null, null, null));
        assertThat(d.shouldRecordKeepaliveExpiry(null, shardFailures), is(true));

        AtomicLong clock2 = new AtomicLong(0L);
        SearchContextKeepaliveDeadline innerWindow = new SearchContextKeepaliveDeadline(clock2::get);
        innerWindow.recordSuccessfulExtension(timeValueMillis(100));
        clock2.set(99L);
        assertThat(innerWindow.shouldRecordKeepaliveExpiry(null, shardFailures), is(false));
        clock2.set(101L);
        assertThat(innerWindow.shouldRecordKeepaliveExpiry(null, shardFailures), is(true));
    }

    /** Past deadline alone does not record; failures must expose {@link SearchContextMissingException}. */
    public void testDoesNotRecordWhenPastDeadlineWithoutMissingContext() {
        AtomicLong clock = new AtomicLong(0L);
        SearchContextKeepaliveDeadline d = new SearchContextKeepaliveDeadline(clock::get);
        d.recordSuccessfulExtension(timeValueMillis(10));
        clock.set(100L);
        assertThat(d.shouldRecordKeepaliveExpiry(null, emptyList()), is(false));
    }

    /** {@link SearchContextMissingException} nested under another root failure still satisfies the heuristic when past deadline. */
    public void testShardFailureWrappedCauseStillDetected() {
        AtomicLong clock = new AtomicLong(500L);
        SearchContextKeepaliveDeadline d = new SearchContextKeepaliveDeadline(clock::get);
        d.recordSuccessfulExtension(timeValueMillis(10)); // deadline 510
        clock.set(600L);
        Exception wrapped = new RuntimeException(missingContext());
        assertThat(d.shouldRecordKeepaliveExpiry(wrapped, emptyList()), is(true));
    }

    /** Shard-level failure reasons are unwrapped like catastrophic failures when past deadline. */
    public void testPaginatedSearchFailureWrappedMissingContextDetected() {
        AtomicLong clock = new AtomicLong(100L);
        SearchContextKeepaliveDeadline d = new SearchContextKeepaliveDeadline(clock::get);
        d.recordSuccessfulExtension(timeValueMillis(10)); // deadline 110
        clock.set(200L);
        List<PaginatedSearchFailure> shardFailures = List.of(
            new PaginatedSearchFailure(new RuntimeException(missingContext()), null, null, null)
        );
        assertThat(d.shouldRecordKeepaliveExpiry(null, shardFailures), is(true));
    }
}
