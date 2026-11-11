/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class SatelliteDeclarationCacheTests extends ESTestCase {

    private static final Map<String, Object> DECLARATION = Map.of("specVersions", List.of(0));

    private final AtomicLong now = new AtomicLong();
    private final AtomicInteger fetches = new AtomicInteger();

    private SatelliteDeclarationCache cache(TimeValue ttl) {
        return new SatelliteDeclarationCache(ttl, now::get);
    }

    /** What a satellite without a declaration id answers. */
    private Map<String, Object> fetch() {
        fetches.incrementAndGet();
        return new HashMap<>(DECLARATION);
    }

    /** What a satellite that gives the id {@code id} in its declaration answers. */
    private Supplier<Map<String, Object>> fetchWithId(String id) {
        return () -> {
            fetches.incrementAndGet();
            Map<String, Object> declaration = new HashMap<>(DECLARATION);
            declaration.put("declarationId", id);
            return declaration;
        };
    }

    private void advance(TimeValue time) {
        now.addAndGet(time.nanos());
    }

    public void testAFreshDeclarationWithoutAnIdIsNotFetchedAgain() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        assertEquals(DECLARATION, cache.resolve("flight://h:1", "t", null, this::fetch));
        advance(TimeValue.timeValueSeconds(59));
        assertEquals(DECLARATION, cache.resolve("flight://h:1", "t", null, this::fetch));
        assertEquals(1, fetches.get());
    }

    public void testAnExpiredDeclarationWithoutAnIdIsFetchedAgain() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        cache.resolve("flight://h:1", "t", null, this::fetch);
        advance(TimeValue.timeValueSeconds(60));
        cache.resolve("flight://h:1", "t", null, this::fetch);
        assertEquals(2, fetches.get());
    }

    public void testADeclarationIsCurrentWhileTheSatelliteGivesItsIdWithinTheTimeTheIdIsTrustedFor() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        advance(TimeValue.timeValueMinutes(59));
        Map<String, Object> declaration = cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        assertEquals("abc", declaration.get("declarationId"));
        assertEquals("the time to live does not apply while the id is given", 1, fetches.get());
    }

    /** A satellite whose id misses an input of its declaration must not keep a node on a stale copy for ever. */
    public void testAnIdIsTrustedOnlyForABoundedTime() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        advance(TimeValue.timeValueSeconds(60L * SatelliteDeclarationCache.ID_TRUST_FACTOR - 1));
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        assertEquals(1, fetches.get());
        advance(TimeValue.timeValueSeconds(1));
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        assertEquals("read again once the id is no longer trusted, though it did not change", 2, fetches.get());
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        assertEquals("and trusted again for another while", 2, fetches.get());
    }

    public void testAServerWithoutADeclarationIsKeptUnderTheIdItGivesInItsSchema() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        for (int i = 0; i < 3; i++) {
            assertNull(cache.resolve("flight://h:1", "t", "abc", () -> {
                fetches.incrementAndGet();
                return null;
            }));
        }
        assertEquals("a satellite that shares no spec version is not asked on every resolution", 1, fetches.get());
        assertNull(cache.resolve("flight://h:1", "t", "def", () -> {
            fetches.incrementAndGet();
            return null;
        }));
        assertEquals("until its id changes", 2, fetches.get());
    }

    public void testTheOldestEntryGoesWhenTheCacheIsFull() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        for (int i = 0; i < SatelliteDeclarationCache.MAX_ENTRIES; i++) {
            advance(TimeValue.timeValueMillis(1));
            cache.resolve("flight://h:1", "t" + i, "abc", fetchWithId("abc"));
        }
        assertEquals(SatelliteDeclarationCache.MAX_ENTRIES, fetches.get());
        advance(TimeValue.timeValueMillis(1));
        cache.resolve("flight://h:1", "one-more", "abc", fetchWithId("abc"));
        assertEquals(SatelliteDeclarationCache.MAX_ENTRIES + 1, fetches.get());
        cache.resolve("flight://h:1", "t1", "abc", fetchWithId("abc"));
        cache.resolve("flight://h:1", "t" + (SatelliteDeclarationCache.MAX_ENTRIES - 1), "abc", fetchWithId("abc"));
        assertEquals("the others are still kept", SatelliteDeclarationCache.MAX_ENTRIES + 1, fetches.get());
        cache.resolve("flight://h:1", "t0", "abc", fetchWithId("abc"));
        assertEquals("the oldest is the one that went", SatelliteDeclarationCache.MAX_ENTRIES + 2, fetches.get());
    }

    public void testResolvingTheSameTargetFromManyThreadsKeepsACurrentCopy() throws Exception {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        int threads = between(2, 8);
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> all = new ArrayList<>();
        AtomicInteger wrong = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            Thread thread = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 200; i++) {
                        String id = i % 2 == 0 ? "abc" : "def";
                        if (id.equals(cache.resolve("flight://h:1", "t", id, fetchWithId(id)).get("declarationId")) == false) {
                            wrong.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            all.add(thread);
            thread.start();
        }
        start.countDown();
        for (Thread thread : all) {
            thread.join();
        }
        assertEquals("a resolution never gets the declaration of another id than the one the satellite gave", 0, wrong.get());
    }

    public void testADeclarationIsReadAgainTheMomentTheIdChanges() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        Map<String, Object> declaration = cache.resolve("flight://h:1", "t", "def", fetchWithId("def"));
        assertEquals("def", declaration.get("declarationId"));
        assertEquals(2, fetches.get());
        cache.resolve("flight://h:1", "t", "def", fetchWithId("def"));
        assertEquals("and the new one is kept", 2, fetches.get());
    }

    /** The answer is kept under the id it carries: if the satellite changed between the two calls, the next resolution notices. */
    public void testAnAnswerIsKeptUnderTheIdItCarriesNotTheOneSeenBefore() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        cache.resolve("flight://h:1", "t", "old", fetchWithId("new"));
        cache.resolve("flight://h:1", "t", "new", fetchWithId("new"));
        assertEquals("the id seen now is the one the kept answer carries", 1, fetches.get());
        cache.resolve("flight://h:1", "t", "old", fetchWithId("new"));
        assertEquals("an id that is not the one the kept answer carries reads again", 2, fetches.get());
    }

    public void testASatelliteThatStopsGivingAnIdFallsBackToTheTimeToLive() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        advance(TimeValue.timeValueSeconds(59));
        cache.resolve("flight://h:1", "t", null, fetchWithId("abc"));
        assertEquals(1, fetches.get());
        advance(TimeValue.timeValueSeconds(1));
        cache.resolve("flight://h:1", "t", null, fetchWithId("abc"));
        assertEquals(2, fetches.get());
    }

    public void testASatelliteThatStartsGivingAnIdIsReadOnceAndThenKept() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        cache.resolve("flight://h:1", "t", null, this::fetch);
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        assertEquals("a copy without an id is not current for an id that is given", 2, fetches.get());
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        assertEquals(2, fetches.get());
    }

    public void testAnIdThatIsNotValidIsNoId() {
        assertNull(SatelliteDeclarationCache.validId(null));
        assertNull(SatelliteDeclarationCache.validId(""));
        assertNull(SatelliteDeclarationCache.validId("a b"));
        assertNull(SatelliteDeclarationCache.validId("é"));
        assertNull(SatelliteDeclarationCache.validId("x".repeat(129)));
        assertEquals("x".repeat(128), SatelliteDeclarationCache.validId("x".repeat(128)));
        assertEquals("!~", SatelliteDeclarationCache.validId("!~"));
    }

    public void testADeclarationIdThatIsNotValidIsTakenForNone() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("a b"));
        advance(TimeValue.timeValueSeconds(61));
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("a b"));
        assertEquals("the copy has no usable id, so the time to live decides", 2, fetches.get());
    }

    public void testAZeroTimeToLiveFetchesEveryTime() {
        SatelliteDeclarationCache cache = cache(TimeValue.ZERO);
        assertFalse(cache.enabled());
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        assertEquals(2, fetches.get());
    }

    public void testInvalidateMakesTheNextResolutionFetch() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        cache.invalidate("flight://h:1", "t");
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        assertEquals(2, fetches.get());
    }

    public void testEachEndpointAndTargetHasItsOwnEntry() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        cache.resolve("flight://h:1", "t", null, this::fetch);
        cache.resolve("flight://h:1", "u", null, this::fetch);
        cache.resolve("flight://h:2", "t", null, this::fetch);
        assertEquals(3, fetches.get());
        cache.invalidate("flight://h:1", "t");
        cache.resolve("flight://h:1", "u", null, this::fetch);
        cache.resolve("flight://h:2", "t", null, this::fetch);
        assertEquals("only the invalidated entry is fetched again", 3, fetches.get());
    }

    public void testAServerWithoutADeclarationIsKeptToo() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        assertNull(cache.resolve("flight://h:1", "t", null, () -> {
            fetches.incrementAndGet();
            return null;
        }));
        assertNull(cache.resolve("flight://h:1", "t", null, this::fetch));
        assertEquals(1, fetches.get());
    }

    public void testAFailedFetchIsNotKept() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        expectThrows(IllegalStateException.class, () -> cache.resolve("flight://h:1", "t", "abc", () -> {
            fetches.incrementAndGet();
            throw new IllegalStateException("the satellite is down");
        }));
        cache.resolve("flight://h:1", "t", "abc", fetchWithId("abc"));
        assertEquals(2, fetches.get());
    }

    /** The stamp adds what depends on the table to the declaration it is given: that must not change the copy kept here. */
    public void testTheCallerGetsACopyItMayAddTo() {
        SatelliteDeclarationCache cache = cache(TimeValue.timeValueSeconds(60));
        cache.resolve("flight://h:1", "t", null, this::fetch).put("totalRecords", 5L);
        cache.resolve("flight://h:1", "t", null, this::fetch).put("totalRecords", 6L);
        assertEquals(DECLARATION, cache.resolve("flight://h:1", "t", null, this::fetch));
        assertEquals(1, fetches.get());
    }
}
