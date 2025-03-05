/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContentFragment;

import java.util.Arrays;
import java.util.Map;

public class TransportStatsTests extends ESTestCase {
    public void testToXContent() {
        final var histogram = new long[HandlingTimeTracker.BUCKET_COUNT];
        histogram[4] = 10;

        final var requestSizeHistogram = new long[29];
        requestSizeHistogram[2] = 9;
        requestSizeHistogram[4] = 10;

        final var responseSizeHistogram = new long[29];
        responseSizeHistogram[3] = 13;
        responseSizeHistogram[5] = 14;

        final var exampleActionStats = new TransportActionStats(7, 8, requestSizeHistogram, 11, 12, responseSizeHistogram);

        assertEquals(
            Strings.toString(
                new TransportStats(
                    1,
                    2,
                    3,
                    ByteSizeUnit.MB.toBytes(4),
                    5,
                    ByteSizeUnit.MB.toBytes(6),
                    histogram,
                    histogram,
                    Map.of("internal:test/action", exampleActionStats)
                ),
                false,
                true
            ),
            Strings.format("""
                {"transport":{"server_open":1,"total_outbound_connections":2,\
                "rx_count":3,"rx_size":"4mb","rx_size_in_bytes":4194304,\
                "tx_count":5,"tx_size":"6mb","tx_size_in_bytes":6291456,\
                "inbound_handling_time_histogram":[{"ge":"8ms","ge_millis":8,"lt":"16ms","lt_millis":16,"count":10}],\
                "outbound_handling_time_histogram":[{"ge":"8ms","ge_millis":8,"lt":"16ms","lt_millis":16,"count":10}],\
                "actions":{"internal:test/action":%s}}}""", Strings.toString(exampleActionStats, false, true))
        );
    }

    private static void assertHistogram(long[] histogram, String expectedJson) {
        assertEquals(expectedJson, Strings.toString((ToXContentFragment) (builder, params) -> {
            TransportStats.histogramToXContent(builder, histogram, "h");
            return builder;
        }, false, true));
    }

    public void testHistogram() {
        final var histogram = new long[HandlingTimeTracker.BUCKET_COUNT];

        assertHistogram(histogram, """
            {"h":[]}""");

        histogram[0] = 10;
        assertHistogram(histogram, """
            {"h":[{"lt":"1ms","lt_millis":1,"count":10}]}""");

        histogram[0] = 0;
        histogram[4] = 10;
        assertHistogram(histogram, """
            {"h":[{"ge":"8ms","ge_millis":8,"lt":"16ms","lt_millis":16,"count":10}]}""");

        histogram[6] = 20;
        assertHistogram(histogram, """
            {"h":[\
            {"ge":"8ms","ge_millis":8,"lt":"16ms","lt_millis":16,"count":10},\
            {"ge":"16ms","ge_millis":16,"lt":"32ms","lt_millis":32,"count":0},\
            {"ge":"32ms","ge_millis":32,"lt":"64ms","lt_millis":64,"count":20}\
            ]}""");

        histogram[0] = 30;
        assertHistogram(histogram, """
            {"h":[\
            {"lt":"1ms","lt_millis":1,"count":30},\
            {"ge":"1ms","ge_millis":1,"lt":"2ms","lt_millis":2,"count":0},\
            {"ge":"2ms","ge_millis":2,"lt":"4ms","lt_millis":4,"count":0},\
            {"ge":"4ms","ge_millis":4,"lt":"8ms","lt_millis":8,"count":0},\
            {"ge":"8ms","ge_millis":8,"lt":"16ms","lt_millis":16,"count":10},\
            {"ge":"16ms","ge_millis":16,"lt":"32ms","lt_millis":32,"count":0},\
            {"ge":"32ms","ge_millis":32,"lt":"64ms","lt_millis":64,"count":20}\
            ]}""");

        Arrays.fill(histogram, 0L);
        histogram[HandlingTimeTracker.BUCKET_COUNT - 1] = 5;
        assertHistogram(histogram, """
            {"h":[{"ge":"1m","ge_millis":65536,"count":5}]}""");

        histogram[HandlingTimeTracker.BUCKET_COUNT - 3] = 6;
        assertHistogram(histogram, """
            {"h":[\
            {"ge":"16.3s","ge_millis":16384,"lt":"32.7s","lt_millis":32768,"count":6},\
            {"ge":"32.7s","ge_millis":32768,"lt":"1m","lt_millis":65536,"count":0},\
            {"ge":"1m","ge_millis":65536,"count":5}\
            ]}""");

        Arrays.fill(histogram, 1L);
        assertHistogram(histogram, """
            {"h":[\
            {"lt":"1ms","lt_millis":1,"count":1},\
            {"ge":"1ms","ge_millis":1,"lt":"2ms","lt_millis":2,"count":1},\
            {"ge":"2ms","ge_millis":2,"lt":"4ms","lt_millis":4,"count":1},\
            {"ge":"4ms","ge_millis":4,"lt":"8ms","lt_millis":8,"count":1},\
            {"ge":"8ms","ge_millis":8,"lt":"16ms","lt_millis":16,"count":1},\
            {"ge":"16ms","ge_millis":16,"lt":"32ms","lt_millis":32,"count":1},\
            {"ge":"32ms","ge_millis":32,"lt":"64ms","lt_millis":64,"count":1},\
            {"ge":"64ms","ge_millis":64,"lt":"128ms","lt_millis":128,"count":1},\
            {"ge":"128ms","ge_millis":128,"lt":"256ms","lt_millis":256,"count":1},\
            {"ge":"256ms","ge_millis":256,"lt":"512ms","lt_millis":512,"count":1},\
            {"ge":"512ms","ge_millis":512,"lt":"1s","lt_millis":1024,"count":1},\
            {"ge":"1s","ge_millis":1024,"lt":"2s","lt_millis":2048,"count":1},\
            {"ge":"2s","ge_millis":2048,"lt":"4s","lt_millis":4096,"count":1},\
            {"ge":"4s","ge_millis":4096,"lt":"8.1s","lt_millis":8192,"count":1},\
            {"ge":"8.1s","ge_millis":8192,"lt":"16.3s","lt_millis":16384,"count":1},\
            {"ge":"16.3s","ge_millis":16384,"lt":"32.7s","lt_millis":32768,"count":1},\
            {"ge":"32.7s","ge_millis":32768,"lt":"1m","lt_millis":65536,"count":1},\
            {"ge":"1m","ge_millis":65536,"count":1}\
            ]}""");
    }

}
