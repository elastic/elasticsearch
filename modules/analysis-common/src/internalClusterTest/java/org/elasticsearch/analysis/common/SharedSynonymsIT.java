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
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersRequest;
import org.elasticsearch.action.admin.indices.analyze.TransportReloadAnalyzersAction;
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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Indices with synonyms_set filters each own their analyzer and share its components only when they loaded identical
 * rules, so loads, reloads and lenient fallbacks affect exactly the indices they would without sharing.
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

    private void reload(String index) {
        client().execute(TransportReloadAnalyzersAction.TYPE, new ReloadAnalyzersRequest(null, false, index)).actionGet();
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
        assertThat(analyze("b", "hello"), contains("hello", "hi"));
        // Each index owns its analyzer: b's load leaves a on its fallback until a is reloaded, as without sharing.
        assertThat(analyze("a", "hello"), contains("hello"));
        assertNotSame("a degraded load is never shared", analyzer("a").getComponents(), analyzer("b").getComponents());
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
        assertThat(analyze("b", "bye"), contains("bye", "ciao"));
        // a keeps its partial load until reloaded, as without sharing.
        assertThat(analyze("a", "bye"), contains("bye"));
        assertNotSame("a degraded load is never shared", analyzer("a").getComponents(), analyzer("b").getComponents());
    }

    public void testSynonymsUpdateReachesEverySharer() throws IOException {
        assumeTrue("analyzer sharing feature flag disabled", AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled());

        putSet("s", "hello, hi");
        createIndex("a", synonymsSetIndex("s"));
        createIndex("b", synonymsSetIndex("s"));
        assertNotSame("each index owns its analyzer", analyzer("a"), analyzer("b"));
        assertSame("identical rules share components", analyzer("a").getComponents(), analyzer("b").getComponents());

        putSet("s", "bye, ciao", true);

        assertThat(analyze("a", "bye"), contains("bye", "ciao"));
        assertThat(analyze("b", "bye"), contains("bye", "ciao"));
        assertSame("still shared after the reload", analyzer("a").getComponents(), analyzer("b").getComponents());
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

    /**
     * The {@code NodeConstruction} wiring: a cluster-settings update, not a direct registry call, stops and resumes analyzer sharing.
     */
    public void testSharingEscapeHatchWiredToClusterSettings() {
        assumeTrue("analyzer sharing feature flag disabled", AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled());
        AnalysisRegistry registry = getInstanceFromNode(AnalysisRegistry.class);
        Settings recipe = Settings.builder()
            .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.my_analyzer.filter", "lowercase")
            .build();

        createIndex("a", recipe);
        createIndex("b", recipe);
        assertThat(registry.cacheHits(), greaterThan(0L));

        try {
            updateClusterSettings(Settings.builder().put(AnalysisRegistry.SHARED_ANALYZERS_ENABLED_SETTING.getKey(), false).build());
            // Baseline after disabling, so background index creation cannot race it.
            long hitsAfterDisable = registry.cacheHits();
            createIndex("c", recipe);
            assertEquals(hitsAfterDisable, registry.cacheHits());
        } finally {
            updateClusterSettings(Settings.builder().putNull(AnalysisRegistry.SHARED_ANALYZERS_ENABLED_SETTING.getKey()).build());
        }

        long hitsAfterEnable = registry.cacheHits();
        createIndex("d", recipe);
        assertThat(registry.cacheHits(), greaterThan(hitsAfterEnable));
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

    /** An update made without a reload reaches the reloaded index and indices created after it, not other indices. */
    public void testDeferredUpdateReachesOnlyReloadedAndNewIndices() throws IOException {
        assumeTrue("analyzer sharing feature flag disabled", AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled());

        putSet("s", "hello, hi");
        createIndex("a", synonymsSetIndex("s"));
        createIndex("b", synonymsSetIndex("s"));
        putSet("s", "bye, ciao");
        createIndex("c", synonymsSetIndex("s"));
        reload("a");

        assertThat(analyze("a", "bye"), contains("bye", "ciao"));
        assertThat(analyze("c", "bye"), contains("bye", "ciao"));
        assertThat(analyze("b", "bye"), contains("bye"));
    }

    public void testDegradedReloadAffectsOnlyTheReloadedIndex() throws IOException {
        assumeTrue("analyzer sharing feature flag disabled", AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled());

        putSet("s", "hello, hi");
        createIndex("a", synonymsSetIndex("s"));
        createIndex("b", synonymsSetIndex("s"));
        putSet("s", "bye, ciao");
        String limit = HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey();
        updateClusterSettings(Settings.builder().put(limit, "0b").build());
        try {
            reload("a");
        } finally {
            updateClusterSettings(Settings.builder().putNull(limit).build());
        }

        assertThat(analyze("a", "hello"), contains("hello"));
        assertThat(analyze("b", "hello"), contains("hello", "hi"));
        createIndex("d", synonymsSetIndex("s"));
        assertThat(analyze("d", "bye"), contains("bye", "ciao"));
    }

    public void testSynonymsUpdateReachesHiddenIndex() throws IOException {
        putSet("s", "hello, hi");
        createIndex("h", Settings.builder().put(synonymsSetIndex("s")).put("index.hidden", true).build());
        // The synonym filter wrapped in a multiplexer.
        createIndex(
            "hm",
            Settings.builder()
                .put(synonymsSetIndex("s"))
                .put("index.hidden", true)
                .put("index.analysis.filter.syn.type", "synonym")
                .put("index.analysis.filter.mux.type", "multiplexer")
                .putList("index.analysis.filter.mux.filters", "syn")
                .putList("index.analysis.analyzer.a.filter", "mux")
                .build()
        );

        putSet("s", "bye, ciao", true);

        assertThat(analyze("h", "bye"), contains("bye", "ciao"));
        assertThat(analyze("hm", "bye"), hasItem("ciao"));
    }
}
