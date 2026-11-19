/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.reindex.AbstractAsyncBulkByPaginatedSearchAction.PaginatedSearchConsumableHitsResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;

import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * A finished page that is still referenced, because the callback that asks for the next page captures it, must not keep its hits
 * reachable. Releasing a hit is not enough: a hit that came from a remote cluster keeps its source after release, so the page has to
 * forget the hits themselves. What the next search needs (scroll id, search-after values, PIT id) has to stay readable.
 */
public class PaginatedHitSourceDropHitsTests extends ESTestCase {

    /**
     * The scroll id, search-after values and PIT id the next search is built from.
     */
    record Cursor(String scrollId, Object[] searchAfter, BytesReference pitId) {
        static Cursor random() {
            Object[] searchAfter = randomBoolean() ? new Object[] { randomLong() } : new Object[] { randomAlphaOfLength(8), randomLong() };
            return new Cursor(randomAlphaOfLength(between(1, 20)), searchAfter, new BytesArray(randomByteArrayOfLength(between(1, 32))));
        }
    }

    /**
     * Stands in for what a hit source hands the worker: the page, and a {@code done} that the worker calls later. Holding one of these
     * is how a finished page stays referenced while the worker waits.
     */
    static final class HeldAsyncResponse implements PaginatedHitSource.AsyncResponse {
        private final PaginatedHitSource.Response response;
        private final Runnable continuation;
        final AtomicInteger doneCalls = new AtomicInteger();
        volatile Cursor cursorSeenByDone;

        HeldAsyncResponse(PaginatedHitSource.Response response, Runnable continuation) {
            this.response = response;
            this.continuation = continuation;
        }

        @Override
        public PaginatedHitSource.Response response() {
            return response;
        }

        @Override
        public void done(TimeValue extraKeepAlive) {
            doneCalls.incrementAndGet();
            // The next search is built from these and nothing else on the page.
            cursorSeenByDone = new Cursor(response.getScrollId(), response.getSearchAfterValues(), response.getPitId());
            continuation.run();
        }
    }

    /**
     * Fails unless every referent has been garbage collected. {@code System.gc()} is only a hint and weak references are cleared
     * asynchronously, so this retries.
     */
    static void assertUnreachable(String what, List<WeakReference<Object>> referents) throws Exception {
        assertBusy(() -> {
            System.gc();
            for (WeakReference<Object> referent : referents) {
                assertNull(what + " is still reachable", referent.get());
            }
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Builds a page of already released hits and records weak references to every hit and every source. The caller must not keep the
     * list or any hit, so this is a method of its own and nothing but the response and the weak references leave it.
     *
     * @param releases bumped once for each remote style hit that was released
     */
    private static PaginatedHitSource.Response releasedPage(
        int hitCount,
        Cursor cursor,
        boolean explicitSearchAfter,
        List<WeakReference<Object>> weak,
        AtomicInteger releases
    ) {
        List<PaginatedHitSource.Hit> hits = new ArrayList<>();
        for (int i = 0; i < hitCount; i++) {
            BytesReference source = new BytesArray(
                ("{\"f\":\"" + randomAlphaOfLength(between(1, 200)) + "\"}").getBytes(StandardCharsets.UTF_8)
            );
            Object[] sortValues = randomBoolean() ? null : new Object[] { randomLong() };
            PaginatedHitSource.Hit hit;
            if (randomBoolean()) {
                hit = new RemoteStyleHit("index-" + i, "id-" + i + randomAlphaOfLength(4), randomLong(), source, sortValues, releases);
            } else {
                SearchHit searchHit = new SearchHit(randomInt(), "id-" + i + randomAlphaOfLength(4));
                searchHit.sourceRef(source);
                hit = new ClientHit(searchHit);
                searchHit.decRef();
            }
            hit.release();
            weak.add(new WeakReference<>(hit));
            weak.add(new WeakReference<>(source));
            hits.add(hit);
        }
        return new PaginatedHitSource.Response(
            randomBoolean(),
            List.of(),
            hitCount + between(0, 1_000),
            hits,
            cursor.scrollId(),
            explicitSearchAfter ? cursor.searchAfter() : null,
            cursor.pitId()
        );
    }

    /**
     * Same as {@link #releasedPage} but the hits are unreleased until the worker's own bookkeeping releases them, which is what the
     * consumable page does. Returns the page that wraps a response held by {@code held}.
     */
    private static PaginatedSearchConsumableHitsResponse unreleasedPage(
        int hitCount,
        Cursor cursor,
        List<WeakReference<Object>> weak,
        AtomicInteger releases,
        HeldAsyncResponse[] heldOut
    ) {
        List<PaginatedHitSource.Hit> hits = new ArrayList<>();
        for (int i = 0; i < hitCount; i++) {
            BytesReference source = new BytesArray(randomByteArrayOfLength(between(1, 200)));
            RemoteStyleHit hit = new RemoteStyleHit("index", "id-" + i, 1L, source, new Object[] { randomLong() }, releases);
            weak.add(new WeakReference<>(hit));
            weak.add(new WeakReference<>(source));
            hits.add(hit);
        }
        PaginatedHitSource.Response response = new PaginatedHitSource.Response(
            false,
            List.of(),
            hitCount,
            hits,
            cursor.scrollId(),
            cursor.searchAfter(),
            cursor.pitId()
        );
        heldOut[0] = new HeldAsyncResponse(response, () -> {});
        return new PaginatedSearchConsumableHitsResponse(heldOut[0]);
    }

    /**
     * Once a page's hits are released and dropped, nothing that still holds the response can reach the hits or their sources.
     * Remote hits keep their source after release, so release alone is not enough.
     */
    public void testDroppingHitsOfAReleasedPageMakesHitsAndSourcesUnreachable() throws Exception {
        List<WeakReference<Object>> weak = new ArrayList<>();
        AtomicInteger releases = new AtomicInteger();
        PaginatedHitSource.Response response = releasedPage(between(1, 10), Cursor.random(), randomBoolean(), weak, releases);
        HeldAsyncResponse held = new HeldAsyncResponse(response, () -> {});

        response.dropHits();

        assertUnreachable("a hit or source of the dropped page", weak);
        assertThat(held.response().getHits(), empty());
        assertThat(held.response(), sameInstance(response));
    }

    /**
     * The next search is built from the scroll id, the search-after values and the PIT id, so dropping the hits must not lose any
     * of them, nor the page level facts the final response reports.
     */
    public void testDroppingHitsKeepsWhatTheNextSearchNeeds() {
        Cursor cursor = Cursor.random();
        int hitCount = between(1, 10);
        boolean timedOut = randomBoolean();
        long totalHits = hitCount + between(0, 1_000);
        List<PaginatedSearchFailure> failures = randomBoolean()
            ? List.of()
            : List.of(new PaginatedSearchFailure(new IllegalStateException(randomAlphaOfLength(5))));
        List<PaginatedHitSource.Hit> hits = new ArrayList<>();
        for (int i = 0; i < hitCount; i++) {
            hits.add(new RemoteStyleHit("index", "id-" + i, 1L, new BytesArray(randomByteArrayOfLength(8)), null, new AtomicInteger()));
        }
        PaginatedHitSource.Response response = new PaginatedHitSource.Response(
            timedOut,
            failures,
            totalHits,
            hits,
            cursor.scrollId(),
            cursor.searchAfter(),
            cursor.pitId()
        );

        response.dropHits();

        assertThat(response.getHits(), empty());
        assertThat(response.getScrollId(), equalTo(cursor.scrollId()));
        assertThat(response.getSearchAfterValues(), equalTo(cursor.searchAfter()));
        assertThat(response.getPitId(), equalTo(cursor.pitId()));
        assertThat(response.isTimedOut(), equalTo(timedOut));
        assertThat(response.getTotalHits(), equalTo(totalHits));
        assertThat(response.getFailures(), sameInstance(failures));
    }

    /**
     * A remote PIT response carries no explicit search-after values, they are the sort values of its last hit. A caller that reads
     * them after the page was dropped still has to get those values, because the next PIT search cannot be built without them.
     */
    public void testSearchAfterValuesTakenFromTheLastHitSurviveDroppingHits() {
        int hitCount = between(1, 10);
        List<PaginatedHitSource.Hit> hits = new ArrayList<>();
        Object[] lastSortValues = null;
        for (int i = 0; i < hitCount; i++) {
            lastSortValues = new Object[] { randomAlphaOfLength(6), randomLong() };
            hits.add(
                new RemoteStyleHit("index", "id-" + i, 1L, new BytesArray(randomByteArrayOfLength(8)), lastSortValues, new AtomicInteger())
            );
        }
        BytesReference pitId = new BytesArray(randomByteArrayOfLength(between(1, 16)));
        PaginatedHitSource.Response response = new PaginatedHitSource.Response(false, List.of(), hitCount, hits, null, null, pitId);
        assertThat(response.getSearchAfterValues(), equalTo(lastSortValues));

        response.dropHits();

        assertThat(response.getSearchAfterValues(), equalTo(lastSortValues));
        assertThat(response.getPitId(), equalTo(pitId));
    }

    /**
     * An empty page has no hits to forget. Dropping it must not fail and must leave the cursor alone.
     */
    public void testDroppingTheHitsOfAnEmptyPageKeepsTheCursor() {
        Cursor cursor = Cursor.random();
        PaginatedHitSource.Response response = new PaginatedHitSource.Response(
            randomBoolean(),
            List.of(),
            0,
            new ArrayList<PaginatedHitSource.Hit>(),
            cursor.scrollId(),
            cursor.searchAfter(),
            cursor.pitId()
        );

        response.dropHits();

        assertThat(response.getHits(), empty());
        assertThat(response.getScrollId(), equalTo(cursor.scrollId()));
        assertThat(response.getSearchAfterValues(), equalTo(cursor.searchAfter()));
        assertThat(response.getPitId(), equalTo(cursor.pitId()));
    }

    /**
     * The worker holds the consumable page, and through it the response. Once every hit is consumed and released, dropping the page
     * leaves neither the page nor the response able to reach a hit or its source, and the cursor is still there for {@code done}.
     */
    public void testDroppingAConsumedPageMakesHitsUnreachableThroughThePageAndTheResponse() throws Exception {
        List<WeakReference<Object>> weak = new ArrayList<>();
        AtomicInteger releases = new AtomicInteger();
        Cursor cursor = Cursor.random();
        int hitCount = between(1, 10);
        HeldAsyncResponse[] held = new HeldAsyncResponse[1];
        PaginatedSearchConsumableHitsResponse page = unreleasedPage(hitCount, cursor, weak, releases, held);

        if (randomBoolean()) {
            page.consumeRemainingHits().forEach(PaginatedHitSource.Hit::release);
        } else {
            page.releaseRemainingHits();
        }
        assertThat(releases.get(), equalTo(hitCount));
        assertFalse(page.hasRemainingHits());

        page.dropHits();

        assertUnreachable("a hit or source of the dropped page", weak);
        assertThat(page.remainingHits(), equalTo(0));
        assertThat(page.response().getHits(), empty());
        assertThat(held[0].response().getHits(), empty());
        assertThat(page.response().getScrollId(), equalTo(cursor.scrollId()));
        assertThat(page.response().getSearchAfterValues(), equalTo(cursor.searchAfter()));
        assertThat(page.response().getPitId(), equalTo(cursor.pitId()));
        // done is still usable and sees the same cursor.
        page.done(TimeValue.ZERO);
        assertThat(held[0].doneCalls.get(), equalTo(1));
        assertThat(held[0].cursorSeenByDone.scrollId(), equalTo(cursor.scrollId()));
        assertThat(held[0].cursorSeenByDone.searchAfter(), equalTo(cursor.searchAfter()));
        assertThat(held[0].cursorSeenByDone.pitId(), equalTo(cursor.pitId()));
    }

    /**
     * Forgetting a page's hits is not a second release. Each hit is released once, by whoever finished with it, and dropping the page
     * afterwards, or asking for the remaining hits to be released, must not release it again.
     */
    public void testDroppingAReleasedPageDoesNotReleaseItsHitsAgain() {
        AtomicInteger releases = new AtomicInteger();
        int hitCount = between(1, 10);
        HeldAsyncResponse[] held = new HeldAsyncResponse[1];
        PaginatedSearchConsumableHitsResponse page = unreleasedPage(hitCount, Cursor.random(), new ArrayList<>(), releases, held);
        page.releaseRemainingHits();
        assertThat(releases.get(), equalTo(hitCount));

        page.dropHits();
        page.releaseRemainingHits();

        assertThat(releases.get(), equalTo(hitCount));
    }
}
