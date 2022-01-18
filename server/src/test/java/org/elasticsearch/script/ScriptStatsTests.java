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
        List<ScriptContextStats> contextStats = List.of(
            new ScriptContextStats("contextB", 302, new TimeSeries(1000, 1001, 1002, 100), new TimeSeries(2000, 2001, 2002, 201)),
            new ScriptContextStats("contextA", 3020, new TimeSeries(1000), new TimeSeries(2010))
        );
        ScriptStats stats = new ScriptStats(contextStats);
        final XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String expected = """
            {
              "script" : {
                "compilations" : 1100,
                "cache_evictions" : 2211,
                "compilation_limit_triggered" : 3322,
                "contexts" : [
                  {
                    "context" : "contextA",
                    "compilations" : 1000,
                    "cache_evictions" : 2010,
                    "compilation_limit_triggered" : 3020
                  },
                  {
                    "context" : "contextB",
                    "compilations" : 100,
                    "compilations_history" : {
                      "5m" : 1000,
                      "15m" : 1001,
                      "24h" : 1002
                    },
                    "cache_evictions" : 201,
                    "cache_evictions_history" : {
                      "5m" : 2000,
                      "15m" : 2001,
                      "24h" : 2002
                    },
                    "compilation_limit_triggered" : 302
                  }
                ]
              }
            }""";
        assertThat(Strings.toString(builder), equalTo(expected));
    }

    public void testSerializeEmptyTimeSeries() throws IOException {
        ScriptContextStats stats = new ScriptContextStats("c", 3333, new TimeSeries(1111), new TimeSeries(2222));

        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String expected = """
            {
              "context" : "c",
              "compilations" : 1111,
              "cache_evictions" : 2222,
              "compilation_limit_triggered" : 3333
            }""";

        assertThat(Strings.toString(builder), equalTo(expected));
    }

    public void testSerializeTimeSeries() throws IOException {
        Function<TimeSeries, ScriptContextStats> mkContextStats = (ts) -> new ScriptContextStats("c", 3333, new TimeSeries(1111), ts);

        TimeSeries series = new TimeSeries(0, 0, 5, 2222);
        String format = """
            {
              "context" : "c",
              "compilations" : 1111,
              "cache_evictions" : %s,
              "cache_evictions_history" : {
                "5m" : %s,
                "15m" : %s,
                "24h" : %s
              },
              "compilation_limit_triggered" : 3333
            }""";

        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        mkContextStats.apply(series).toXContent(builder, ToXContent.EMPTY_PARAMS);

        assertThat(Strings.toString(builder), equalTo(String.format(Locale.ROOT, format, 2222, 0, 0, 5)));

        series = new TimeSeries(0, 7, 1234, 5678);
        builder = XContentFactory.jsonBuilder().prettyPrint();
        mkContextStats.apply(series).toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), equalTo(String.format(Locale.ROOT, format, 5678, 0, 7, 1234)));

        series = new TimeSeries(123, 456, 789, 91011);
        builder = XContentFactory.jsonBuilder().prettyPrint();
        mkContextStats.apply(series).toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), equalTo(String.format(Locale.ROOT, format, 91011, 123, 456, 789)));
    }

    public void testTimeSeriesIsEmpty() {
        assertTrue((new TimeSeries(0, 0, 0, 0)).areTimingsEmpty());
        assertTrue((new TimeSeries(0, 0, 0, 123)).areTimingsEmpty());
        long total = randomLongBetween(1, 1024);
        long day = randomLongBetween(1, total);
        long fifteen = day >= 1 ? randomLongBetween(0, day) : 0;
        long five = fifteen >= 1 ? randomLongBetween(0, fifteen) : 0;
        assertFalse((new TimeSeries(five, fifteen, day, 0)).areTimingsEmpty());
    }

    public void testTimeSeriesSerialization() throws IOException {
        ScriptContextStats stats = randomStats();

        ScriptContextStats deserStats = serDeser(Version.V_8_0_0, Version.V_7_16_0, stats);
        assertEquals(stats.getCompilations(), deserStats.getCompilations());
        assertEquals(stats.getCacheEvictions(), deserStats.getCacheEvictions());
        assertEquals(stats.getCompilationLimitTriggered(), deserStats.getCompilationLimitTriggered());
        assertTrue(deserStats.getCompilationsHistory().areTimingsEmpty());
        assertEquals(stats.getCompilations(), deserStats.getCompilationsHistory().total);
        assertTrue(deserStats.getCacheEvictionsHistory().areTimingsEmpty());
        assertEquals(stats.getCacheEvictions(), deserStats.getCacheEvictionsHistory().total);

        deserStats = serDeser(Version.V_8_0_0, Version.V_8_0_0, stats);
        assertEquals(stats.getCompilations(), deserStats.getCompilations());
        assertEquals(stats.getCacheEvictions(), deserStats.getCacheEvictions());
        assertEquals(stats.getCompilationLimitTriggered(), deserStats.getCompilationLimitTriggered());
        assertEquals(stats.getCompilationsHistory(), deserStats.getCompilationsHistory());
        assertEquals(stats.getCacheEvictionsHistory(), deserStats.getCacheEvictionsHistory());

        deserStats = serDeser(Version.V_8_1_0, Version.V_7_16_0, stats);
        assertEquals(stats.getCompilations(), deserStats.getCompilations());
        assertEquals(stats.getCacheEvictions(), deserStats.getCacheEvictions());
        assertEquals(stats.getCompilationLimitTriggered(), deserStats.getCompilationLimitTriggered());
        assertEquals(new TimeSeries(stats.getCompilationsHistory().total), deserStats.getCompilationsHistory());
        assertEquals(new TimeSeries(stats.getCacheEvictionsHistory().total), deserStats.getCacheEvictionsHistory());

        deserStats = serDeser(Version.V_8_1_0, Version.V_8_1_0, stats);
        assertEquals(stats.getCompilations(), deserStats.getCompilations());
        assertEquals(stats.getCacheEvictions(), deserStats.getCacheEvictions());
        assertEquals(stats.getCompilationLimitTriggered(), deserStats.getCompilationLimitTriggered());
        assertEquals(stats.getCompilationsHistory(), deserStats.getCompilationsHistory());
        assertEquals(stats.getCacheEvictionsHistory(), deserStats.getCacheEvictionsHistory());
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
        List<TimeSeries> timeSeries = new ArrayList<>();
        for (int j = 0; j < 2; j++) {
            if (randomBoolean() && histStats[j] > 0) {
                long day = randomLongBetween(0, histStats[j]);
                long fifteen = day >= 1 ? randomLongBetween(0, day) : 0;
                long five = fifteen >= 1 ? randomLongBetween(0, fifteen) : 0;
                timeSeries.add(new TimeSeries(five, fifteen, day, histStats[j]));
            } else {
                timeSeries.add(new TimeSeries(histStats[j]));
            }
        }
        return new ScriptContextStats(randomAlphaOfLength(15), randomLongBetween(0, 1024), timeSeries.get(0), timeSeries.get(1));
    }
}
