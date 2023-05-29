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

import static org.elasticsearch.threadpool.ThreadPool.THREAD_POOL_TYPES;
import static org.hamcrest.Matchers.contains;

public class ThreadPoolStatsTests extends ESTestCase {
    public void testThreadPoolStatsSort() {
        var unorderedStats = new ArrayList<ThreadPoolStats.Stats>();

        unorderedStats.add(new ThreadPoolStats.Stats("z", 7, 0, 0, 0, 0, 0L));
        unorderedStats.add(new ThreadPoolStats.Stats("m", 5, 0, 0, 0, 0, 0L));
        unorderedStats.add(new ThreadPoolStats.Stats("m", -3, 0, 0, 0, 0, 0L));
        unorderedStats.add(new ThreadPoolStats.Stats("d", 2, 0, 0, 0, 0, 0L));
        unorderedStats.add(new ThreadPoolStats.Stats("m", 4, 0, 0, 0, 0, 0L));
        unorderedStats.add(new ThreadPoolStats.Stats("t", 6, 0, 0, 0, 0, 0L));
        unorderedStats.add(new ThreadPoolStats.Stats("a", -1, 0, 0, 0, 0, 0L));

        var copy = new ArrayList<>(unorderedStats);
        Collections.sort(copy);

        assertThat(copy.stream().map(ThreadPoolStats.Stats::name).toList(), contains("a", "d", "m", "m", "m", "t", "z"));
        assertThat(copy.stream().map(ThreadPoolStats.Stats::threads).toList(), contains(-1, 2, -3, 4, 5, 6, 7));

        // assert that the ThreadPoolStats constructor sorts the stat list
        var threadPoolStats = new ThreadPoolStats(unorderedStats);
        assertThat(threadPoolStats.stats().stream().map(ThreadPoolStats.Stats::name).toList(), contains("a", "d", "m", "m", "m", "t", "z"));
        assertThat(threadPoolStats.stats().stream().map(ThreadPoolStats.Stats::threads).toList(), contains(-1, 2, -3, 4, 5, 6, 7));
    }

    public void testMergeThreadPoolStats() {
        var firstStats = randomList(1, 20, ThreadPoolStatsTests::randomStats);
        var secondStats = randomList(1, 20, ThreadPoolStatsTests::randomStats);

        var fullStats = new ArrayList<>(firstStats);
        fullStats.addAll(secondStats);

        var target = new ThreadPoolStats(fullStats);
        var expected = ThreadPoolStats.merge(new ThreadPoolStats(firstStats), new ThreadPoolStats(secondStats));

        assertNotSame(target, expected);
        assertEquals(target, expected);
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
        return randomBoolean() ? -1 : randomIntBetween(0, Integer.MAX_VALUE);
    }
}
