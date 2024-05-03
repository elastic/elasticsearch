/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContentFragment;

import java.util.Arrays;

public class TransportActionStatsTests extends ESTestCase {

    public void testToXContent() {
        final var requestSizeHistogram = new long[29];
        requestSizeHistogram[2] = 9;
        requestSizeHistogram[4] = 10;

        final var responseSizeHistogram = new long[29];
        responseSizeHistogram[3] = 13;
        responseSizeHistogram[5] = 14;

        assertEquals("""
            {"requests":{"count":7,"total_size":"8b","total_size_in_bytes":8,"histogram":[\
            {"ge":"16b","ge_bytes":16,"lt":"32b","lt_bytes":32,"count":9},\
            {"ge":"32b","ge_bytes":32,"lt":"64b","lt_bytes":64,"count":0},\
            {"ge":"64b","ge_bytes":64,"lt":"128b","lt_bytes":128,"count":10}\
            ]},"responses":{"count":11,"total_size":"12b","total_size_in_bytes":12,"histogram":[\
            {"ge":"32b","ge_bytes":32,"lt":"64b","lt_bytes":64,"count":13},\
            {"ge":"64b","ge_bytes":64,"lt":"128b","lt_bytes":128,"count":0},\
            {"ge":"128b","ge_bytes":128,"lt":"256b","lt_bytes":256,"count":14}\
            ]}}""", Strings.toString(new TransportActionStats(7, 8, requestSizeHistogram, 11, 12, responseSizeHistogram), false, true));
    }

    private static void assertHistogram(long[] histogram, String expectedJson) {
        assertEquals(expectedJson, Strings.toString((ToXContentFragment) (builder, params) -> {
            TransportActionStats.histogramToXContent(builder, histogram);
            return builder;
        }, false, true));
    }

    public void testHistogram() {
        final var histogram = new long[29];

        assertHistogram(histogram, """
            {"histogram":[]}""");

        histogram[0] = 10;
        assertHistogram(histogram, """
            {"histogram":[{"lt":"8b","lt_bytes":8,"count":10}]}""");

        histogram[0] = 0;
        histogram[4] = 10;
        assertHistogram(histogram, """
            {"histogram":[{"ge":"64b","ge_bytes":64,"lt":"128b","lt_bytes":128,"count":10}]}""");

        histogram[6] = 20;
        assertHistogram(histogram, """
            {"histogram":[\
            {"ge":"64b","ge_bytes":64,"lt":"128b","lt_bytes":128,"count":10},\
            {"ge":"128b","ge_bytes":128,"lt":"256b","lt_bytes":256,"count":0},\
            {"ge":"256b","ge_bytes":256,"lt":"512b","lt_bytes":512,"count":20}\
            ]}""");

        histogram[0] = 30;
        assertHistogram(histogram, """
            {"histogram":[\
            {"lt":"8b","lt_bytes":8,"count":30},\
            {"ge":"8b","ge_bytes":8,"lt":"16b","lt_bytes":16,"count":0},\
            {"ge":"16b","ge_bytes":16,"lt":"32b","lt_bytes":32,"count":0},\
            {"ge":"32b","ge_bytes":32,"lt":"64b","lt_bytes":64,"count":0},\
            {"ge":"64b","ge_bytes":64,"lt":"128b","lt_bytes":128,"count":10},\
            {"ge":"128b","ge_bytes":128,"lt":"256b","lt_bytes":256,"count":0},\
            {"ge":"256b","ge_bytes":256,"lt":"512b","lt_bytes":512,"count":20}\
            ]}""");

        Arrays.fill(histogram, 0L);
        histogram[histogram.length - 1] = 5;
        assertHistogram(histogram, """
            {"histogram":[{"ge":"1gb","ge_bytes":1073741824,"count":5}]}""");

        histogram[histogram.length - 3] = 6;
        assertHistogram(histogram, """
            {"histogram":[\
            {"ge":"256mb","ge_bytes":268435456,"lt":"512mb","lt_bytes":536870912,"count":6},\
            {"ge":"512mb","ge_bytes":536870912,"lt":"1gb","lt_bytes":1073741824,"count":0},\
            {"ge":"1gb","ge_bytes":1073741824,"count":5}\
            ]}""");

        Arrays.fill(histogram, 1L);
        assertHistogram(histogram, """
            {"histogram":[\
            {"lt":"8b","lt_bytes":8,"count":1},\
            {"ge":"8b","ge_bytes":8,"lt":"16b","lt_bytes":16,"count":1},\
            {"ge":"16b","ge_bytes":16,"lt":"32b","lt_bytes":32,"count":1},\
            {"ge":"32b","ge_bytes":32,"lt":"64b","lt_bytes":64,"count":1},\
            {"ge":"64b","ge_bytes":64,"lt":"128b","lt_bytes":128,"count":1},\
            {"ge":"128b","ge_bytes":128,"lt":"256b","lt_bytes":256,"count":1},\
            {"ge":"256b","ge_bytes":256,"lt":"512b","lt_bytes":512,"count":1},\
            {"ge":"512b","ge_bytes":512,"lt":"1kb","lt_bytes":1024,"count":1},\
            {"ge":"1kb","ge_bytes":1024,"lt":"2kb","lt_bytes":2048,"count":1},\
            {"ge":"2kb","ge_bytes":2048,"lt":"4kb","lt_bytes":4096,"count":1},\
            {"ge":"4kb","ge_bytes":4096,"lt":"8kb","lt_bytes":8192,"count":1},\
            {"ge":"8kb","ge_bytes":8192,"lt":"16kb","lt_bytes":16384,"count":1},\
            {"ge":"16kb","ge_bytes":16384,"lt":"32kb","lt_bytes":32768,"count":1},\
            {"ge":"32kb","ge_bytes":32768,"lt":"64kb","lt_bytes":65536,"count":1},\
            {"ge":"64kb","ge_bytes":65536,"lt":"128kb","lt_bytes":131072,"count":1},\
            {"ge":"128kb","ge_bytes":131072,"lt":"256kb","lt_bytes":262144,"count":1},\
            {"ge":"256kb","ge_bytes":262144,"lt":"512kb","lt_bytes":524288,"count":1},\
            {"ge":"512kb","ge_bytes":524288,"lt":"1mb","lt_bytes":1048576,"count":1},\
            {"ge":"1mb","ge_bytes":1048576,"lt":"2mb","lt_bytes":2097152,"count":1},\
            {"ge":"2mb","ge_bytes":2097152,"lt":"4mb","lt_bytes":4194304,"count":1},\
            {"ge":"4mb","ge_bytes":4194304,"lt":"8mb","lt_bytes":8388608,"count":1},\
            {"ge":"8mb","ge_bytes":8388608,"lt":"16mb","lt_bytes":16777216,"count":1},\
            {"ge":"16mb","ge_bytes":16777216,"lt":"32mb","lt_bytes":33554432,"count":1},\
            {"ge":"32mb","ge_bytes":33554432,"lt":"64mb","lt_bytes":67108864,"count":1},\
            {"ge":"64mb","ge_bytes":67108864,"lt":"128mb","lt_bytes":134217728,"count":1},\
            {"ge":"128mb","ge_bytes":134217728,"lt":"256mb","lt_bytes":268435456,"count":1},\
            {"ge":"256mb","ge_bytes":268435456,"lt":"512mb","lt_bytes":536870912,"count":1},\
            {"ge":"512mb","ge_bytes":536870912,"lt":"1gb","lt_bytes":1073741824,"count":1},\
            {"ge":"1gb","ge_bytes":1073741824,"count":1}\
            ]}""");
    }
}
