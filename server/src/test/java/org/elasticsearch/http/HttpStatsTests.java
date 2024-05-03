/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
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
        final Map<String, HttpRouteStats> m = new HashMap<>(first.httpRouteStats());
        second.httpRouteStats().forEach((k, v) -> m.merge(k, v, HttpRouteStats::merge));
        assertEquals(merged.httpRouteStats(), m);
    }

    public void testToXContent() {
        final var requestSizeHistogram = new long[32];
        requestSizeHistogram[2] = 9;
        requestSizeHistogram[4] = 10;

        final var responseSizeHistogram = new long[32];
        responseSizeHistogram[3] = 13;
        responseSizeHistogram[5] = 14;

        final var responseTimeHistogram = new long[18];
        responseTimeHistogram[4] = 17;
        responseTimeHistogram[6] = 18;

        final HttpRouteStats httpRouteStats = new HttpRouteStats(
            1,
            ByteSizeUnit.MB.toBytes(2),
            requestSizeHistogram,
            3,
            ByteSizeUnit.MB.toBytes(4),
            responseSizeHistogram,
            responseTimeHistogram
        );

        assertThat(
            Strings.toString(new HttpStats(1, 2, List.of(), Map.of("http/path", httpRouteStats)), false, true),
            equalTo(
                Strings.format(
                    """
                        {"http":{"current_open":1,"total_opened":2,"clients":[],"routes":{"http/path":%s}}}""",
                    Strings.toString(httpRouteStats, false, true)
                )
            )
        );
    }

    public static HttpStats randomHttpStats() {
        return new HttpStats(
            randomLongBetween(0, Long.MAX_VALUE),
            randomLongBetween(0, Long.MAX_VALUE),
            IntStream.range(1, randomIntBetween(2, 10)).mapToObj(HttpStatsTests::randomClients).toList(),
            randomMap(1, 3, () -> new Tuple<>(randomAlphaOfLength(10), randomHttpRouteStats()))
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

    public static HttpRouteStats randomHttpRouteStats() {
        return new HttpRouteStats(
            randomLongBetween(0, 99),
            randomLongBetween(0, 9999),
            IntStream.range(0, 32).mapToLong(i -> randomLongBetween(0, 42)).toArray(),
            randomLongBetween(0, 99),
            randomLongBetween(0, 9999),
            IntStream.range(0, 32).mapToLong(i -> randomLongBetween(0, 42)).toArray(),
            IntStream.range(0, 18).mapToLong(i -> randomLongBetween(0, 42)).toArray()
        );
    }
}
