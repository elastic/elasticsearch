/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.test.ESTestCase;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.hasSize;

public class HttpStatsTests extends ESTestCase {

    public void testMerge() {
        var first = randomHttpStats();
        var second = randomHttpStats();

        var merged = HttpStats.merge(first, second);

        assertEquals(merged.getServerOpen(), first.getServerOpen() + second.getServerOpen());
        assertEquals(merged.getTotalOpen(), first.getTotalOpen() + second.getTotalOpen());
        assertThat(merged.getClientStats(), hasSize(first.getClientStats().size() + second.getClientStats().size()));
        assertEquals(merged.getClientStats(), Stream.concat(first.getClientStats().stream(), second.getClientStats().stream()).toList());
    }

    public static HttpStats randomHttpStats() {
        return new HttpStats(
            randomLongBetween(0, Long.MAX_VALUE),
            randomLongBetween(0, Long.MAX_VALUE),
            IntStream.range(1, randomIntBetween(2, 10)).mapToObj(HttpStatsTests::randomClients).toList()
        );
    }

    public static HttpStats.ClientStats randomClients(int i) {
        return new HttpStats.ClientStats(
            randomInt(),
            randomAlphaOfLength(100),
            randomAlphaOfLength(100),
            randomAlphaOfLength(100),
            randomAlphaOfLength(100),
            randomAlphaOfLength(100),
            randomAlphaOfLength(100),
            randomLong(),
            randomLong(),
            randomLong(),
            randomLong(),
            randomLong()
        );
    }
}
