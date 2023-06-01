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

public class ThreadPoolStatsTests extends ESTestCase {
    public void testThreadPoolStatsSort() {
        var stats = List.of(
            new ThreadPoolStats.Stats("z", -1, 0, 0, 0, 0, 0L),
            new ThreadPoolStats.Stats("m", 3, 0, 0, 0, 0, 0L),
            new ThreadPoolStats.Stats("m", 1, 0, 0, 0, 0, 0L),
            new ThreadPoolStats.Stats("d", -1, 0, 0, 0, 0, 0L),
            new ThreadPoolStats.Stats("m", 2, 0, 0, 0, 0, 0L),
            new ThreadPoolStats.Stats("t", -1, 0, 0, 0, 0, 0L),
            new ThreadPoolStats.Stats("a", -1, 0, 0, 0, 0, 0L)
        );

        var copy = new ArrayList<>(stats);
        Collections.sort(copy);

        var names = copy.stream().map(ThreadPoolStats.Stats::name).toList();
        assertThat(names, contains("a", "d", "m", "m", "m", "t", "z"));

        var threads = copy.stream().map(ThreadPoolStats.Stats::threads).toList();
        assertThat(threads, contains(-1, -1, 1, 2, 3, -1, -1));
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
