/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.synonyms.PutSynonymsAction;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.ReloadableCustomAnalyzer;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Indices whose synonyms_set filters share one analyzer. A synonyms API update must reach every sharer. A lenient
 * filter whose sets cannot all be loaded at recovery falls back to the rules it could load without failing; that
 * fallback must not count as loaded, so an index that joins once the sets are available retries the load, which
 * rebuilds the shared instance for every sharer.
 */
public class SharedSynonymsIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // MapperExtrasPlugin: the .synonyms index maps rules as match_only_text.
        return List.of(CommonAnalysisPlugin.class, MapperExtrasPlugin.class);
    }

    private static Settings synonymsSetIndex(String... sets) {
        return Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "syn")
            .put("index.analysis.filter.syn.type", "synonym_graph")
            .putList("index.analysis.filter.syn.synonyms_set", sets)
            .put("index.analysis.filter.syn.updateable", true)
            .build();
    }

    /** Creates a set without reloading analyzers, so only a recovery load can pick it up. */
    private void putSet(String set, String rule) throws IOException {
        putSet(set, rule, false);
    }

    private void putSet(String set, String rule, boolean reloadAnalyzers) throws IOException {
        // append=true avoids delete-by-query, which needs the reindex module.
        String body = "{\"synonyms_set\":[{\"synonyms\":\"" + rule + "\"}]}";
        client().execute(
            PutSynonymsAction.INSTANCE,
            new PutSynonymsAction.Request(set, reloadAnalyzers, true, new BytesArray(body), XContentType.JSON)
        ).actionGet();
    }

    private List<String> analyze(String index, String text) {
        AnalyzeAction.Response response = indicesAdmin().prepareAnalyze(index, text).setAnalyzer("a").get();
        return response.getTokens().stream().map(AnalyzeAction.AnalyzeToken::getTerm).sorted().toList();
    }

    private ReloadableCustomAnalyzer analyzer(String index) {
        var analyzer = underlyingAnalyzer(index);
        assertThat(analyzer, instanceOf(ReloadableCustomAnalyzer.class));
        return (ReloadableCustomAnalyzer) analyzer;
    }

    private Analyzer underlyingAnalyzer(String index) {
        return getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(index)).getIndexAnalyzers().get("a").analyzer();
    }

    /**
     * A non-updateable lenient filter whose map trips the circuit breaker falls back to an empty map. Nothing reloads a
     * non-updateable analyzer, so an index created once the breaker clears must build its own rather than share the fallback.
     */
    public void testIndexAfterCircuitBreakerFallbackDoesNotShareIt() throws Exception {
        assumeTrue("analyzer sharing feature flag disabled", AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled());

        Settings index = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "syn")
            .put("index.analysis.filter.syn.type", "synonym_graph")
            .putList("index.analysis.filter.syn.synonyms", "hello, hi")
            .put("index.analysis.filter.syn.lenient", true)
            .build();
        String limit = HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey();
        updateClusterSettings(Settings.builder().put(limit, "0b").build());
        try {
            createIndex("a", index);
        } finally {
            updateClusterSettings(Settings.builder().putNull(limit).build());
        }
        assertThat(analyze("a", "hello"), contains("hello"));

        createIndex("b", index);
        assertNotSame(underlyingAnalyzer("a"), underlyingAnalyzer("b"));
        assertThat(analyze("b", "hello"), contains("hello", "hi"));

        // The detached fallback is released, not leaked, once its index goes.
        assertAcked(indicesAdmin().prepareDelete("a", "b"));
        assertBusy(() -> getInstanceFromNode(IndicesService.class).getAnalysis().assertNoCachedEntries());
    }

    public void testIndexJoiningEmptyFallbackRetriesLoad() throws IOException {
        assumeTrue("analyzer sharing feature flag disabled", AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled());

        // Set "s" does not exist yet: the lenient recovery load falls back to empty rules.
        createIndex("a", synonymsSetIndex("s"));
        assertThat(analyze("a", "hello"), contains("hello"));

        putSet("s", "hello, hi");

        createIndex("b", synonymsSetIndex("s"));
        assertSame("b must share a's analyzer for this test to exercise sharing", analyzer("a"), analyzer("b"));
        assertThat(analyze("b", "hello"), contains("hello", "hi"));
        // b's retry rebuilt the shared instance, so a sees the set too.
        assertThat(analyze("a", "hello"), contains("hello", "hi"));
    }

    public void testIndexJoiningPartialFallbackRetriesLoad() throws IOException {
        assumeTrue("analyzer sharing feature flag disabled", AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled());

        // "s1" exists but "s2" does not: the lenient recovery load succeeds with s1's rules only.
        putSet("s1", "hello, hi");
        createIndex("a", synonymsSetIndex("s1", "s2"));
        assertThat(analyze("a", "hello"), contains("hello", "hi"));
        assertThat(analyze("a", "bye"), contains("bye"));

        putSet("s2", "bye, ciao");

        createIndex("b", synonymsSetIndex("s1", "s2"));
        assertSame("b must share a's analyzer for this test to exercise sharing", analyzer("a"), analyzer("b"));
        assertThat(analyze("b", "bye"), contains("bye", "ciao"));
        assertThat(analyze("a", "bye"), contains("bye", "ciao"));
    }

    public void testSynonymsUpdateReachesEverySharer() throws IOException {
        assumeTrue("analyzer sharing feature flag disabled", AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled());

        putSet("s", "hello, hi");
        createIndex("a", synonymsSetIndex("s"));
        createIndex("b", synonymsSetIndex("s"));
        assertSame("b must share a's analyzer for this test to exercise sharing", analyzer("a"), analyzer("b"));

        putSet("s", "bye, ciao", true);

        assertThat(analyze("a", "bye"), contains("bye", "ciao"));
        assertThat(analyze("b", "bye"), contains("bye", "ciao"));
    }

    /**
     * With both {@code synonyms} and {@code synonyms_path} set, the rules come from the file. Indices whose files differ must not
     * share, even though their inline lists match.
     */
    public void testInlineRulesWithDifferentFilesDoNotShare() throws IOException {
        assumeTrue("analyzer sharing feature flag disabled", AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled());

        Path config = getInstanceFromNode(Environment.class).configDir();
        Files.createDirectories(config);
        Files.writeString(config.resolve("one.txt"), "hello, hi");
        Files.writeString(config.resolve("two.txt"), "hello, hey");

        createIndex("a", inlineWithPathIndex("one.txt"));
        createIndex("b", inlineWithPathIndex("two.txt"));
        assertThat(analyze("a", "hello"), contains("hello", "hi"));
        assertThat(analyze("b", "hello"), contains("hello", "hey"));
    }

    private static Settings inlineWithPathIndex(String path) {
        return Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "syn")
            .put("index.analysis.filter.syn.type", "synonym_graph")
            .putList("index.analysis.filter.syn.synonyms", "foo, bar")
            .put("index.analysis.filter.syn.synonyms_path", path)
            .build();
    }
}
