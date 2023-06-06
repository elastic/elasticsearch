/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.TransportVersion;
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
import java.util.function.Function;

import static org.elasticsearch.script.ScriptContextStatsTests.randomScriptContextStats;
import static org.elasticsearch.script.TimeSeriesTests.randomTimeseries;
import static org.hamcrest.Matchers.equalTo;

public class ScriptStatsTests extends ESTestCase {
    public void testXContentChunked() throws IOException {
        List<ScriptContextStats> contextStats = List.of(
            new ScriptContextStats("contextB", 302, new TimeSeries(1000, 1001, 1002, 100), new TimeSeries(2000, 2001, 2002, 201)),
            new ScriptContextStats("contextA", 3020, new TimeSeries(1000), new TimeSeries(2010))
        );
        ScriptStats stats = ScriptStats.read(contextStats);
        final XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();

        builder.startObject();
        for (var it = stats.toXContentChunked(ToXContent.EMPTY_PARAMS); it.hasNext();) {
            it.next().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
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

        assertThat(Strings.toString(builder), equalTo(Strings.format(format, 2222, 0, 0, 5)));

        series = new TimeSeries(0, 7, 1234, 5678);
        builder = XContentFactory.jsonBuilder().prettyPrint();
        mkContextStats.apply(series).toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), equalTo(Strings.format(format, 5678, 0, 7, 1234)));

        series = new TimeSeries(123, 456, 789, 91011);
        builder = XContentFactory.jsonBuilder().prettyPrint();
        mkContextStats.apply(series).toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), equalTo(Strings.format(format, 91011, 123, 456, 789)));
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

        ScriptContextStats deserStats = serDeser(TransportVersion.V_8_0_0, TransportVersion.V_7_16_0, stats);
        // Due to how the versions are handled by TimeSeries serialization, we cannot just simply assert that both object are
        // equals but not the same
        assertEquals(stats.getCompilations(), deserStats.getCompilations());
        assertEquals(stats.getCacheEvictions(), deserStats.getCacheEvictions());
        assertEquals(stats.getCompilationLimitTriggered(), deserStats.getCompilationLimitTriggered());
        assertTrue(deserStats.getCompilationsHistory().areTimingsEmpty());
        assertEquals(stats.getCompilations(), deserStats.getCompilationsHistory().total);
        assertTrue(deserStats.getCacheEvictionsHistory().areTimingsEmpty());
        assertEquals(stats.getCacheEvictions(), deserStats.getCacheEvictionsHistory().total);

        deserStats = serDeser(TransportVersion.V_8_0_0, TransportVersion.V_8_0_0, stats);
        assertNotSame(stats, deserStats);
        assertEquals(stats, deserStats);

        deserStats = serDeser(TransportVersion.V_8_1_0, TransportVersion.V_7_16_0, stats);
        // Due to how the versions are handled by TimeSeries serialization, we cannot just simply assert that both object are
        // equals but not the same
        assertEquals(stats.getCompilations(), deserStats.getCompilations());
        assertEquals(stats.getCacheEvictions(), deserStats.getCacheEvictions());
        assertEquals(stats.getCompilationLimitTriggered(), deserStats.getCompilationLimitTriggered());
        assertEquals(new TimeSeries(stats.getCompilationsHistory().total), deserStats.getCompilationsHistory());
        assertEquals(new TimeSeries(stats.getCacheEvictionsHistory().total), deserStats.getCacheEvictionsHistory());

        deserStats = serDeser(TransportVersion.V_8_1_0, TransportVersion.V_8_1_0, stats);
        assertNotSame(stats, deserStats);
        assertEquals(stats, deserStats);
    }

    public void testMerge() {
        var first = randomScriptStats();
        var second = randomScriptStats();

        System.out.println(Strings.toString(first, true, true));
        System.out.println(Strings.toString(second, true, true));
        System.out.println(Strings.toString(ScriptStats.merge(first, second), true, true));
        assertEquals(
            ScriptStats.merge(first, second),
            new ScriptStats(
                List.of(
                    ScriptContextStats.merge(first.contextStats().get(0), second.contextStats().get(0)),
                    ScriptContextStats.merge(first.contextStats().get(1), second.contextStats().get(1)),
                    ScriptContextStats.merge(first.contextStats().get(2), second.contextStats().get(2))
                ),
                first.compilations() + second.compilations(),
                first.cacheEvictions() + second.cacheEvictions(),
                first.compilationLimitTriggered() + second.compilationLimitTriggered(),
                TimeSeries.merge(first.compilationsHistory(), second.compilationsHistory()),
                TimeSeries.merge(first.cacheEvictionsHistory(), second.cacheEvictionsHistory())
            )
        );
    }

    public static ScriptStats randomScriptStats() {
        return new ScriptStats(
            List.of(randomScriptContextStats("context-a"), randomScriptContextStats("context-b"), randomScriptContextStats("context-c")),
            randomLongBetween(0, 10000),
            randomLongBetween(0, 10000),
            randomLongBetween(0, 10000),
            randomTimeseries(),
            randomTimeseries()
        );
    }

    public ScriptContextStats serDeser(TransportVersion outVersion, TransportVersion inVersion, ScriptContextStats stats)
        throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(outVersion);
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(inVersion);
                return ScriptContextStats.read(in);
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
