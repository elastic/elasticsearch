/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.threadpool.ThreadPool.THREAD_POOL_TYPES;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

public class ThreadPoolStatsTests extends ESTestCase {
    public void testThreadPoolStatsConstructorSortTheStats() {
        var unorderedStats = List.of(
            new ThreadPoolStats.Stats("z", 7, 0, 0, 0, 0, 0L),
            new ThreadPoolStats.Stats("m", 5, 0, 0, 0, 0, 0L),
            new ThreadPoolStats.Stats("m", -3, 0, 0, 0, 0, 0L),
            new ThreadPoolStats.Stats("d", 2, 0, 0, 0, 0, 0L),
            new ThreadPoolStats.Stats("m", 4, 0, 0, 0, 0, 0L),
            new ThreadPoolStats.Stats("t", 6, 0, 0, 0, 0, 0L),
            new ThreadPoolStats.Stats("a", -1, 0, 0, 0, 0, 0L)
        );

        var copy = new ArrayList<>(unorderedStats);
        Collections.sort(copy);

        var threadPoolStats = new ThreadPoolStats(unorderedStats);
        assertThat(threadPoolStats.stats().stream().map(ThreadPoolStats.Stats::name).toList(), contains("a", "d", "m", "m", "m", "t", "z"));
        assertThat(threadPoolStats.stats().stream().map(ThreadPoolStats.Stats::threads).toList(), contains(-1, 2, -3, 4, 5, 6, 7));
    }

    public void testMergeThreadPoolStats() {
        var firstStats = List.of(randomStats("name-1"), randomStats("name-2"), randomStats("name-3"));
        var secondStats = List.of(randomStats("name-4"), randomStats("name-5"), randomStats("name-2"), randomStats("name-3"));

        var tps1 = new ThreadPoolStats(firstStats);
        var tps2 = new ThreadPoolStats(secondStats);
        var target = ThreadPoolStats.merge(tps1, tps2);

        assertThat(target.stats(), hasSize(5));
        assertThat(
            target.stats(),
            containsInAnyOrder(
                firstStats.get(0), // name-1
                ThreadPoolStats.Stats.merge(firstStats.get(1), secondStats.get(2)), // name-2
                ThreadPoolStats.Stats.merge(firstStats.get(2), secondStats.get(3)), // name-3
                secondStats.get(0), // name-4
                secondStats.get(1) // name-5
            )
        );
    }

    public void testStatsMerge() {
        assertEquals(ThreadPoolStats.Stats.merge(stats(-1), stats(-1)), stats(-1));
        assertEquals(ThreadPoolStats.Stats.merge(stats(1), stats(-1)), stats(1));
        assertEquals(ThreadPoolStats.Stats.merge(stats(-1), stats(1)), stats(1));
        assertEquals(ThreadPoolStats.Stats.merge(stats(1), stats(2)), stats(3));
    }

    private static ThreadPoolStats.Stats stats(int value) {
        return new ThreadPoolStats.Stats("a", value, value, value, value, value, value);
    }

    public void testSerialization() throws IOException {
        var original = new ThreadPoolStats(randomList(2, ThreadPoolStatsTests::randomStats));
        var other = serialize(original);

        assertNotSame(original, other);
        assertEquals(original, other);
    }

    private static ThreadPoolStats serialize(ThreadPoolStats stats) throws IOException {
        var out = new BytesStreamOutput();
        stats.writeTo(out);
        return new ThreadPoolStats(out.bytes().streamInput());
    }

    public static ThreadPoolStats.Stats randomStats() {
        return randomStats(randomFrom(THREAD_POOL_TYPES.keySet()));
    }

    public static ThreadPoolStats.Stats randomStats(String name) {
        return new ThreadPoolStats.Stats(
            name,
            randomMinusOneOrOther(),
            randomMinusOneOrOther(),
            randomMinusOneOrOther(),
            randomMinusOneOrOther(),
            randomMinusOneOrOther(),
            randomMinusOneOrOther()
        );
    }

    private static int randomMinusOneOrOther() {
        return randomBoolean() ? -1 : randomIntBetween(0, 1000);
    }
}
