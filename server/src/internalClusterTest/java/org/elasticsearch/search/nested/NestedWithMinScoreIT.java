/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.nested;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class NestedWithMinScoreIT extends ESIntegTestCase {

    public static class ScriptTestPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of("score_script", params -> {
                final Object scoreAccessor = params.get("_score");
                if (scoreAccessor instanceof Number) {
                    return ((Number) scoreAccessor).doubleValue();
                } else {
                    return null;
                }
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getMockPlugins());
        plugins.add(ScriptTestPlugin.class);
        return plugins;
    }

    public void testNestedWithMinScore() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        mapping.startObject("properties");
        {
            mapping.startObject("toolTracks");
            {
                mapping.field("type", "nested");
                mapping.startObject("properties");
                {
                    mapping.startObject("data");
                    mapping.field("type", "text");
                    mapping.endObject();

                    mapping.startObject("confidence");
                    mapping.field("type", "double");
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        mapping.endObject();

        client().admin().indices().prepareCreate("test").setMapping(mapping).get();

        XContentBuilder doc = XContentFactory.jsonBuilder();
        doc.startObject();
        doc.startArray("toolTracks");
        double[] confidence = new double[] { 0.3, 0.92, 0.7, 0.85, 0.2, 0.3, 0.75, 0.82, 0.1, 0.6, 0.3, 0.7 };
        for (double v : confidence) {
            doc.startObject();
            doc.field("confidence", v);
            doc.field("data", "cash dispenser, automated teller machine, automatic teller machine");
            doc.endObject();
        }
        doc.endArray();
        doc.endObject();

        client().prepareIndex("test").setId("d1").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).setSource(doc).get();
        final BoolQueryBuilder childQuery = new BoolQueryBuilder().filter(
            new MatchPhraseQueryBuilder("toolTracks.data", "cash dispenser, automated teller machine, automatic teller machine")
        ).filter(new RangeQueryBuilder("toolTracks.confidence").from(0.8));

        final ScriptScoreQueryBuilder scriptScoreQuery = new ScriptScoreQueryBuilder(
            new NestedQueryBuilder("toolTracks", new ConstantScoreQueryBuilder(childQuery), ScoreMode.Total),
            new Script(ScriptType.INLINE, MockScriptPlugin.NAME, "score_script", Map.of())
        );
        scriptScoreQuery.setMinScore(1.0f);
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(scriptScoreQuery);
        source.profile(randomBoolean());
        if (randomBoolean()) {
            source.trackTotalHitsUpTo(randomBoolean() ? Integer.MAX_VALUE : randomIntBetween(1, 1000));
        }
        SearchRequest searchRequest = new SearchRequest("test").source(source);
        final SearchResponse searchResponse = client().search(searchRequest).actionGet();
        ElasticsearchAssertions.assertSearchHits(searchResponse, "d1");
    }
}
