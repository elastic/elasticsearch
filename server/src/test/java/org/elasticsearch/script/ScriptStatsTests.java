/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class ScriptStatsTests extends ESTestCase {
    public void testXContent() throws IOException {
        List<ScriptContextStats> contextStats = org.elasticsearch.core.List.of(
            new ScriptContextStats(
                "contextB",
                100,
                201,
                302,
                new ScriptContextStats.TimeSeries(1000, 1001, 1002),
                new ScriptContextStats.TimeSeries(2000, 2001, 2002)
            ),
            new ScriptContextStats("contextA", 1000, 2010, 3020, null, new ScriptContextStats.TimeSeries(0, 0, 0))
        );
        ScriptStats stats = new ScriptStats(contextStats);
        final XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String expected = "{\n"
            + "  \"script\" : {\n"
            + "    \"compilations\" : 1100,\n"
            + "    \"cache_evictions\" : 2211,\n"
            + "    \"compilation_limit_triggered\" : 3322\n"
            + "  }\n"
            + "}";
        assertThat(Strings.toString(builder), equalTo(expected));
    }

    public void testSerializeEmptyTimeSeries() throws IOException {
        ScriptContextStats.TimeSeries empty = new ScriptContextStats.TimeSeries();
        ScriptContextStats stats = new ScriptContextStats("c", 1111, 2222, 3333, null, empty);

        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String expected = "{\n"
            + "  \"context\" : \"c\",\n"
            + "  \"compilations\" : 1111,\n"
            + "  \"cache_evictions\" : 2222,\n"
            + "  \"compilation_limit_triggered\" : 3333\n"
            + "}";

        assertThat(Strings.toString(builder), equalTo(expected));
    }

    public void testSerializeTimeSeries() throws IOException {
        Function<ScriptContextStats.TimeSeries, ScriptContextStats> mkContextStats = (ts) -> new ScriptContextStats(
            "c",
            1111,
            2222,
            3333,
            null,
            ts
        );

        ScriptContextStats.TimeSeries series = new ScriptContextStats.TimeSeries(0, 0, 5);
        String format = "{\n"
            + "  \"context\" : \"c\",\n"
            + "  \"compilations\" : 1111,\n"
            + "  \"cache_evictions\" : 2222,\n"
            + "  \"cache_evictions_history\" : {\n"
            + "    \"5m\" : %d,\n"
            + "    \"15m\" : %d,\n"
            + "    \"24h\" : %d\n"
            + "  },\n"
            + "  \"compilation_limit_triggered\" : 3333\n"
            + "}";

        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        mkContextStats.apply(series).toXContent(builder, ToXContent.EMPTY_PARAMS);

        assertThat(Strings.toString(builder), equalTo(String.format(Locale.ROOT, format, 0, 0, 5)));

        series = new ScriptContextStats.TimeSeries(0, 7, 1234);
        builder = XContentFactory.jsonBuilder().prettyPrint();
        mkContextStats.apply(series).toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), equalTo(String.format(Locale.ROOT, format, 0, 7, 1234)));

        series = new ScriptContextStats.TimeSeries(123, 456, 789);
        builder = XContentFactory.jsonBuilder().prettyPrint();
        mkContextStats.apply(series).toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), equalTo(String.format(Locale.ROOT, format, 123, 456, 789)));
    }

    public void testTimeSeriesAssertions() {
        expectThrows(AssertionError.class, () -> new ScriptContextStats.TimeSeries(-1, 1, 2));
        expectThrows(AssertionError.class, () -> new ScriptContextStats.TimeSeries(1, 0, 2));
        expectThrows(AssertionError.class, () -> new ScriptContextStats.TimeSeries(1, 3, 2));
    }

    public void testTimeSeriesIsEmpty() {
        assertTrue((new ScriptContextStats.TimeSeries(0, 0, 0)).isEmpty());
        long day = randomLongBetween(1, 1024);
        long fifteen = day >= 1 ? randomLongBetween(0, day) : 0;
        long five = fifteen >= 1 ? randomLongBetween(0, fifteen) : 0;
        assertFalse((new ScriptContextStats.TimeSeries(five, fifteen, day)).isEmpty());
    }

    public void testTimeSeriesSerialization() throws IOException {
        ScriptContextStats stats = randomStats();

        ScriptContextStats deserStats = serDeser(Version.V_7_16_0, Version.V_7_16_0, stats);
        assertEquals(stats.getCompilations(), deserStats.getCompilations());
        assertEquals(stats.getCacheEvictions(), deserStats.getCacheEvictions());
        assertEquals(stats.getCompilationLimitTriggered(), deserStats.getCompilationLimitTriggered());
        assertNull(deserStats.getCompilationsHistory());
        assertNull(deserStats.getCacheEvictionsHistory());
    }

    public ScriptContextStats serDeser(Version outVersion, Version inVersion, ScriptContextStats stats) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(outVersion);
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(inVersion);
                return new ScriptContextStats(in);
            }
        }
    }

    public ScriptContextStats randomStats() {
        long[] histStats = { randomLongBetween(0, 2048), randomLongBetween(0, 2048) };
        List<ScriptContextStats.TimeSeries> timeSeries = new ArrayList<>();
        for (int j = 0; j < 2; j++) {
            if (randomBoolean() && histStats[j] > 0) {
                long day = randomLongBetween(0, histStats[j]);
                long fifteen = day >= 1 ? randomLongBetween(0, day) : 0;
                long five = fifteen >= 1 ? randomLongBetween(0, fifteen) : 0;
                timeSeries.add(new ScriptContextStats.TimeSeries(five, fifteen, day));
            } else {
                timeSeries.add(new ScriptContextStats.TimeSeries());
            }
        }
        return new ScriptContextStats(
            randomAlphaOfLength(15),
            histStats[0],
            histStats[1],
            randomLongBetween(0, 1024),
            timeSeries.get(0),
            timeSeries.get(1)
        );
    }
}
