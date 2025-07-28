/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ScriptCacheStatsTests extends ESTestCase {
    public void testXContentChunkedWithGeneralMode() throws IOException {
        var builder = XContentFactory.jsonBuilder().prettyPrint();
        var contextStats = List.of(
            new ScriptContextStats("context-1", 302, new TimeSeries(1000, 1001, 1002, 100), new TimeSeries(2000, 2001, 2002, 201)),
            new ScriptContextStats("context-2", 3020, new TimeSeries(1000), new TimeSeries(2010))
        );
        var stats = ScriptStats.read(contextStats);
        var scriptCacheStats = new ScriptCacheStats(stats);

        builder.startObject();
        scriptCacheStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String expected = """
            {
              "script_cache" : {
                "sum" : {
                  "compilations" : 1100,
                  "cache_evictions" : 2211,
                  "compilation_limit_triggered" : 3322
                }
              }
            }""";
        assertThat(Strings.toString(builder), equalTo(expected));
    }

    public void testXContentChunkedWithContextMode() throws IOException {
        var builder = XContentFactory.jsonBuilder().prettyPrint();
        var scriptCacheStats = new ScriptCacheStats(
            Map.of(
                "context-1",
                ScriptStats.read(
                    new ScriptContextStats("context-1", 302, new TimeSeries(1000, 1001, 1002, 100), new TimeSeries(2000, 2001, 2002, 201))
                ),
                "context-2",
                ScriptStats.read(new ScriptContextStats("context-2", 3020, new TimeSeries(1000), new TimeSeries(2010)))
            )
        );

        builder.startObject();
        scriptCacheStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String expected = """
            {
              "script_cache" : {
                "sum" : {
                  "compilations" : 1100,
                  "cache_evictions" : 2211,
                  "compilation_limit_triggered" : 3322
                },
                "contexts" : [
                  {
                    "context" : "context-1",
                    "compilations" : 100,
                    "cache_evictions" : 201,
                    "compilation_limit_triggered" : 302
                  },
                  {
                    "context" : "context-2",
                    "compilations" : 1000,
                    "cache_evictions" : 2010,
                    "compilation_limit_triggered" : 3020
                  }
                ]
              }
            }""";
        assertThat(Strings.toString(builder), equalTo(expected));
    }
}
