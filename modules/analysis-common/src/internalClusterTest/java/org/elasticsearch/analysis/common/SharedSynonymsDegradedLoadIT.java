/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.synonyms.PutSynonymsAction;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.ReloadableCustomAnalyzer;
import org.elasticsearch.index.analysis.ReloadableCustomAnalyzer.LoadState;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

/**
 * A lenient synonyms_set filter whose set cannot be loaded at recovery falls back to empty rules without failing.
 * When indices share that analyzer, the fallback must not count as loaded: an index that joins once the set is
 * available retries the load, which rebuilds the shared instance for every sharer.
 */
public class SharedSynonymsDegradedLoadIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // MapperExtrasPlugin: the .synonyms index maps rules as match_only_text.
        return List.of(CommonAnalysisPlugin.class, MapperExtrasPlugin.class);
    }

    private static Settings synonymsSetIndex() {
        return Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "syn")
            .put("index.analysis.filter.syn.type", "synonym_graph")
            .putList("index.analysis.filter.syn.synonyms_set", "s")
            .put("index.analysis.filter.syn.updateable", true)
            .build();
    }

    private List<String> analyze(String index) {
        AnalyzeAction.Response response = indicesAdmin().prepareAnalyze(index, "hello").setAnalyzer("a").get();
        return response.getTokens().stream().map(AnalyzeAction.AnalyzeToken::getTerm).sorted().toList();
    }

    private ReloadableCustomAnalyzer analyzer(String index) {
        var analyzer = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(index))
            .getIndexAnalyzers()
            .get("a")
            .analyzer();
        assertThat(analyzer, instanceOf(ReloadableCustomAnalyzer.class));
        return (ReloadableCustomAnalyzer) analyzer;
    }

    public void testIndexJoiningDegradedAnalyzerRetriesLoad() throws IOException {
        assumeTrue("analyzer sharing feature flag disabled", AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled());

        // Set "s" does not exist yet: the lenient recovery load falls back to empty rules.
        createIndex("a", synonymsSetIndex());
        assertThat(analyze("a"), contains("hello"));
        assertEquals(LoadState.DEGRADED, analyzer("a").loadState());

        // Create the set without reloading analyzers. append=true avoids delete-by-query, which needs the reindex module.
        String body = "{\"synonyms_set\":[{\"synonyms\":\"hello, hi\"}]}";
        client().execute(
            PutSynonymsAction.INSTANCE,
            new PutSynonymsAction.Request("s", false, true, new BytesArray(body), XContentType.JSON)
        ).actionGet();

        createIndex("b", synonymsSetIndex());
        assertSame("b must share a's analyzer for this test to exercise sharing", analyzer("a"), analyzer("b"));
        assertEquals(LoadState.LOADED, analyzer("b").loadState());
        assertThat(analyze("b"), contains("hello", "hi"));
        // b's retry rebuilt the shared instance, so a sees the set too.
        assertThat(analyze("a"), contains("hello", "hi"));
    }
}
