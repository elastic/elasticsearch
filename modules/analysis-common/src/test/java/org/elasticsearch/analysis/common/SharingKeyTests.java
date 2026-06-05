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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalyzerComponents;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.ReloadableCustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * End-to-end paranoid tests for sharingKey behavior in analysis-common factories. Builds real
 * {@link IndexAnalyzers} via {@link AnalysisRegistry} (the path users actually exercise) and
 * verifies that the analyzer cache shares iff the recipe agrees.
 */
public class SharingKeyTests extends ESTestCase {

    private AnalysisRegistry registry;
    private Path configDir;
    private ThreadPool threadPool;

    @Before
    public void setUpRegistry() throws Exception {
        Path home = createTempDir();
        configDir = home.resolve("config");
        Files.createDirectories(configDir);
        threadPool = new TestThreadPool(getTestName());
        // Build CommonAnalysisPlugin with mocked services (needed for the synonyms factory's
        // circuit breaker and cluster service).
        CommonAnalysisPlugin plugin = new TestCommonAnalysisPluginBuilder(threadPool).build();
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        AnalysisModule module = new AnalysisModule(
            TestEnvironment.newEnvironment(nodeSettings),
            List.of(plugin),
            new StablePluginsRegistry()
        );
        registry = module.getAnalysisRegistry();
    }

    private final List<IndexAnalyzers> trackedAnalyzers = new ArrayList<>();

    @After
    public void shutdownAndAssertNoLeaks() throws IOException {
        // Auto-close every IndexAnalyzers handed out by build() and then assert the cache is
        // empty. Catches any test that built an IndexAnalyzers without going through the tracked
        // helper, or any code path in the registry that fails to release on close.
        try {
            IOUtils.close(trackedAnalyzers);
            trackedAnalyzers.clear();
            registry.assertNoCachedEntries();
            registry.close();
        } finally {
            threadPool.shutdownNow();
        }
    }

    private IndexAnalyzers build(Settings indexSettings) throws IOException {
        Settings s = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).put(indexSettings).build();
        IndexSettings is = IndexSettingsModule.newIndexSettings("test", s);
        IndexAnalyzers ia = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, is);
        trackedAnalyzers.add(ia);
        return ia;
    }

    // ---- Custom analyzer with built-in chain components: shares trivially via prebuilt singletons ----

    public void testBuiltInChainSharesAcrossIndices() throws IOException {
        Settings s = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "stop")
            .build();
        IndexAnalyzers a = build(s);
        IndexAnalyzers b = build(s);
        assertSame(a.get("a"), b.get("a"));
    }

    // ---- User-defined custom filters with same recipe ----

    public void testUserDefinedStopFilterSharesAcrossDifferentNames() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.filter.my_stop.type", "stop")
            .putList("index.analysis.filter.my_stop.stopwords", "the", "and", "of")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "my_stop")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.filter.differently_named.type", "stop")
            .putList("index.analysis.filter.differently_named.stopwords", "the", "and", "of")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "differently_named")
            .build();
        assertSame(build(sA).get("a"), build(sB).get("a"));
    }

    public void testUserDefinedNGramFilterShares() throws IOException {
        Settings s = Settings.builder()
            .put("index.analysis.filter.my_ng.type", "ngram")
            .put("index.analysis.filter.my_ng.min_gram", 2)
            .put("index.analysis.filter.my_ng.max_gram", 3)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "my_ng")
            .build();
        assertSame(build(s).get("a"), build(s).get("a"));
    }

    public void testNGramFilterDifferentMinGramDoesNotShare() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.filter.my_ng.type", "ngram")
            .put("index.analysis.filter.my_ng.min_gram", 2)
            .put("index.analysis.filter.my_ng.max_gram", 3)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "my_ng")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.filter.my_ng.type", "ngram")
            .put("index.analysis.filter.my_ng.min_gram", 3)
            .put("index.analysis.filter.my_ng.max_gram", 3)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "my_ng")
            .build();
        assertNotSame(build(sA).get("a"), build(sB).get("a"));
    }

    public void testKeepWordsSameCaseSettingShares() throws IOException {
        Settings s = Settings.builder()
            .put("index.analysis.filter.k.type", "keep")
            .putList("index.analysis.filter.k.keep_words", "apple", "banana")
            .put("index.analysis.filter.k.keep_words_case", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "k")
            .build();
        assertSame(build(s).get("a"), build(s).get("a"));
    }

    /**
     * keep_words_case changes how the keep set matches (case-insensitive vs not). A case-insensitive
     * CharArraySet stores its words lower-cased, so for an already-lower-case word list the stored
     * content is identical for both flag values — only the matching behavior differs. Two otherwise
     * identical keep filters differing only by this flag must therefore NOT share an analyzer
     * instance; keep is an index-time-capable filter, so sharing would change how data is tokenized.
     * Lower-case keep_words is used deliberately: with mixed-case words the stored content would
     * differ on its own and the test would pass even without the fix. Regression for the
     * StableCharArraySet case-sensitivity fix.
     */
    public void testKeepWordsDifferentCaseSettingDoesNotShare() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.filter.k.type", "keep")
            .putList("index.analysis.filter.k.keep_words", "apple", "banana")
            .put("index.analysis.filter.k.keep_words_case", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "k")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.filter.k.type", "keep")
            .putList("index.analysis.filter.k.keep_words", "apple", "banana")
            .put("index.analysis.filter.k.keep_words_case", false)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "k")
            .build();
        assertNotSame(build(sA).get("a"), build(sB).get("a"));
    }

    // ---- Language analyzer provider sharing ----

    public void testEnglishAnalyzerSharesByDefault() throws IOException {
        Settings s = Settings.builder().put("index.analysis.analyzer.a.type", "english").build();
        assertSame(build(s).get("a"), build(s).get("a"));
    }

    public void testEnglishAnalyzerDifferentStopWordsDoesNotShare() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.analyzer.a.type", "english")
            .putList("index.analysis.analyzer.a.stopwords", "the", "a")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.analyzer.a.type", "english")
            .putList("index.analysis.analyzer.a.stopwords", "the", "and")
            .build();
        assertNotSame(build(sA).get("a"), build(sB).get("a"));
    }

    public void testFrenchAnalyzerSharesAcrossDifferentLocalNames() throws IOException {
        Settings sA = Settings.builder().put("index.analysis.analyzer.fr1.type", "french").build();
        Settings sB = Settings.builder().put("index.analysis.analyzer.fr2.type", "french").build();
        assertSame(build(sA).get("fr1"), build(sB).get("fr2"));
    }

    // ---- Pattern factory sharing — Pattern's identity equality is masked by sharingKey ----

    public void testPatternFilterSamePatternShares() throws IOException {
        Settings s = Settings.builder()
            .put("index.analysis.filter.my_pr.type", "pattern_replace")
            .put("index.analysis.filter.my_pr.pattern", "\\d+")
            .put("index.analysis.filter.my_pr.replacement", "N")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "my_pr")
            .build();
        assertSame(build(s).get("a"), build(s).get("a"));
    }

    public void testPatternFilterDifferentPatternDoesNotShare() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.filter.my_pr.type", "pattern_replace")
            .put("index.analysis.filter.my_pr.pattern", "\\d+")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "my_pr")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.filter.my_pr.type", "pattern_replace")
            .put("index.analysis.filter.my_pr.pattern", "\\w+")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "my_pr")
            .build();
        assertNotSame(build(sA).get("a"), build(sB).get("a"));
    }

    // ---- Synonym factory: file-based stamp ----

    public void testInlineSynonymsShare() throws IOException {
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms", "i-pod, ipod, i pod", "universe, cosmos")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        assertSame(build(s).get("a"), build(s).get("a"));
    }

    /**
     * synonym and synonym_graph build different token streams (SynonymFilter vs
     * SynonymGraphFilter) from identical settings. SynonymGraphTokenFilterFactory inherits the
     * sharing key from its superclass, so the key must include the concrete factory type;
     * otherwise the two would collide and an index could be served the wrong filter.
     */
    public void testSynonymAndSynonymGraphDoNotShareWithSameRules() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms", "i-pod, ipod, i pod", "universe, cosmos")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym_graph")
            .putList("index.analysis.filter.syn.synonyms", "i-pod, ipod, i pod", "universe, cosmos")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        assertNotSame("synonym and synonym_graph must never share an analyzer", build(sA).get("a"), build(sB).get("a"));
    }

    public void testInlineSynonymsDifferentRulesDoNotShare() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms", "i-pod, ipod, i pod")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms", "universe, cosmos")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        assertNotSame(build(sA).get("a"), build(sB).get("a"));
    }

    /**
     * Two indices with the SAME inline synonym rules and same behavior, but cosmetically
     * different settings maps (one spells out {@code expand: true}, the other relies on the
     * default which is also true) must still share. The synonym factory's sharing key captures
     * the resolved behavior-affecting values (source, format, expand, lenient, mode, rules), not
     * the raw {@link Settings} blob — so an irrelevant settings difference does not block sharing.
     */
    public void testInlineSynonymsShareDespiteIrrelevantSettingsDifference() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.expand", true) // explicit; matches the default
            .putList("index.analysis.filter.syn.synonyms", "i-pod, ipod, i pod", "universe, cosmos")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym") // expand omitted; defaults to true
            .putList("index.analysis.filter.syn.synonyms", "i-pod, ipod, i pod", "universe, cosmos")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        assertSame("identical synonym behavior must share regardless of cosmetic settings", build(sA).get("a"), build(sB).get("a"));
    }

    // ---- offset_gap is per-index analyzer state and must be part of the analyzer key ----

    public void testSameOffsetGapShares() throws IOException {
        Settings s = Settings.builder()
            .put("index.analysis.analyzer.a.type", "custom")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .put("index.analysis.analyzer.a.offset_gap", 7)
            .build();
        assertSame(build(s).get("a"), build(s).get("a"));
    }

    public void testDifferentOffsetGapDoesNotShare() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.analyzer.a.type", "custom")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .put("index.analysis.analyzer.a.offset_gap", 7)
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.analyzer.a.type", "custom")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .put("index.analysis.analyzer.a.offset_gap", 13)
            .build();
        assertNotSame("analyzers differing only in offset_gap must not share", build(sA).get("a"), build(sB).get("a"));
    }

    public void testSynonymsPathSameFileShares() throws IOException {
        Path file = configDir.resolve("syn.txt");
        Files.writeString(file, "i-pod, ipod\nuniverse, cosmos\n");
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn.txt")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        assertSame(build(s).get("a"), build(s).get("a"));
    }

    public void testSynonymsPathFileEditDoesNotShareAcrossEdit() throws IOException {
        Path file = configDir.resolve("syn.txt");
        Files.writeString(file, "i-pod, ipod\n");
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn.txt")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        NamedAnalyzer beforeAnalyzer = build(s).get("a");

        // The synonyms_path sharing key stamps the file's size and mtime. Editing the file to
        // different content of a different length changes the size, so the stamp (and key) differ
        // deterministically — no need to spin on the filesystem's mtime resolution.
        Files.writeString(file, "i-pod, ipod\nuniverse, cosmos\n");
        assertNotSame("Edited synonyms file must invalidate the cache", beforeAnalyzer, build(s).get("a"));
    }

    /**
     * Updateable (search-time) file synonyms key on path only — NOT content — so two indices built
     * before and after a file edit still share one instance ({@code _reload_search_analyzers}
     * refreshes that instance for everyone). Contrast {@link #testSynonymsPathFileEditDoesNotShareAcrossEdit},
     * where non-updateable (index-time) file synonyms keep a content stamp and must not share across an edit.
     */
    public void testUpdateableSynonymsPathSharesAcrossFileEdit() throws IOException {
        Path file = configDir.resolve("syn_upd.txt");
        Files.writeString(file, "i-pod, ipod\n");
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn_upd.txt")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        NamedAnalyzer first = build(s).get("a");
        Files.writeString(file, "i-pod, ipod, mp3-player\n");  // content changes, path does not
        assertSame("updateable file synonyms share by path regardless of content version", first, build(s).get("a"));
    }

    /**
     * Updateable synonyms_set shares by set name alone — no content generation in the key. Two
     * indices referencing the same set share a single live analyzer instance.
     */
    public void testUpdateableSynonymsSetSharesByName() throws IOException {
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms_set", "my_set")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        assertSame(build(s).get("a"), build(s).get("a"));
    }

    // ---- Custom normalizers share by chain recipe, like custom analyzers ----

    public void testCustomNormalizerSharesByRecipe() throws IOException {
        Settings s = Settings.builder()
            .put("index.analysis.normalizer.norm.type", "custom")
            .putList("index.analysis.normalizer.norm.filter", "lowercase")
            .build();
        assertSame(build(s).getNormalizer("norm"), build(s).getNormalizer("norm"));
    }

    public void testCustomNormalizerDifferentChainDoesNotShare() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.normalizer.norm.type", "custom")
            .putList("index.analysis.normalizer.norm.filter", "lowercase")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.normalizer.norm.type", "custom")
            .putList("index.analysis.normalizer.norm.filter", "lowercase", "asciifolding")
            .build();
        assertNotSame(build(sA).getNormalizer("norm"), build(sB).getNormalizer("norm"));
    }

    // ---- Recipe composition ----
    //
    // The per-factory contract (AnalysisFactoryTestCase) checks each factory in isolation. These
    // tests cover how the registry composes a whole recipe into one AnalyzerKey — that every slot
    // (tokenizer, each char filter, each token filter, in order) contributes. They are properties of
    // the composition, independent of which factories are used, so a small fixed set suffices; no
    // cross-product of factory settings is needed.

    /** Identical multi-component recipes (char filter + tokenizer + two filters) collapse to one instance. */
    public void testIdenticalMultiComponentChainShares() throws IOException {
        Settings s = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.char_filter", "html_strip")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "reverse")
            .build();
        assertSame(build(s).get("a"), build(s).get("a"));
    }

    /** Reordering the filter chain changes the recipe, so the analyzers must not share (order is keyed). */
    public void testFilterOrderDoesNotShare() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "reverse")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "reverse", "lowercase")
            .build();
        assertNotSame(build(sA).get("a"), build(sB).get("a"));
    }

    /** Adding a token filter to the chain changes the recipe. */
    public void testAddingFilterDoesNotShare() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "reverse")
            .build();
        assertNotSame(build(sA).get("a"), build(sB).get("a"));
    }

    /** Adding a char filter changes the recipe (the char-filter slot is part of the key). */
    public void testAddingCharFilterDoesNotShare() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.char_filter", "html_strip")
            .putList("index.analysis.analyzer.a.filter", "lowercase")
            .build();
        assertNotSame(build(sA).get("a"), build(sB).get("a"));
    }

    /**
     * One reload request reaches every index that shares an analyzer, but the shared instance must be
     * rebuilt ONCE for the request — not once per index. The reload token (the request) dedups it; a
     * later request (new token) rebuilds again.
     */
    public void testSharedSynonymsReloadOncePerRequest() throws IOException {
        Path file = configDir.resolve("syn_shared.txt");
        Files.writeString(file, "i-pod, ipod\n");
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn_shared.txt")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        IndexAnalyzers ia1 = build(s);
        IndexAnalyzers ia2 = build(s);
        IndexAnalyzers ia3 = build(s);
        NamedAnalyzer shared = ia1.get("a");
        assertSame(shared, ia2.get("a"));
        assertSame(shared, ia3.get("a"));
        ReloadableCustomAnalyzer rca = (ReloadableCustomAnalyzer) shared.analyzer();

        // One reload request (one token) fans out to all three sharing indices.
        Object request = new Object();
        ia1.reload(registry, settingsFor(s), null, false, request);
        AnalyzerComponents afterFirst = rca.getComponents();
        ia2.reload(registry, settingsFor(s), null, false, request);
        ia3.reload(registry, settingsFor(s), null, false, request);
        assertSame("the shared analyzer must rebuild once per reload request, not once per index", afterFirst, rca.getComponents());

        // A subsequent reload request (new token) rebuilds the shared instance again.
        ia1.reload(registry, settingsFor(s), null, false, new Object());
        assertNotSame("a new reload request must rebuild the shared analyzer", afterFirst, rca.getComponents());
    }

    /**
     * Regression test for the reload/local-name mismatch bug: when two indices share a
     * {@link ReloadableCustomAnalyzer} but each refers to it under a different local name
     * (index A calls it {@code search_a}, index B calls it {@code search_b}),
     * {@code reloadAnalyzerInPlace} looks up the requesting index's settings by
     * {@code currentReference.name()} — which is always the original builder's name
     * ({@code "search_a"}) regardless of which index is currently reloading.
     *
     * <p>If B's shard processes first:
     * <ol>
     *   <li>{@code tryClaimReload(token)} returns {@code true} — the token is claimed.</li>
     *   <li>Settings lookup: {@code B_settings.get("search_a")} → {@code null}.</li>
     *   <li>Silent bail-out — no reload happens.</li>
     *   <li>A's shard arrives with the same token → {@code tryClaimReload} returns {@code false}
     *       → A also skips.</li>
     * </ol>
     * Result: reload is silently missed for the whole broadcast, but the response reports success.
     *
     * <p>This test deliberately calls {@code ib.reload(...)} before {@code ia.reload(...)} to
     * trigger the bug deterministically rather than relying on nondeterministic shard ordering.
     */
    public void testReloadWithDifferentLocalNamesActuallyReloads() throws IOException {
        Path file = configDir.resolve("syn_localnames.txt");
        Files.writeString(file, "i-pod, ipod\n");

        // Same filter recipe, different local analyzer names.
        Settings sA = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn_localnames.txt")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.search_a.tokenizer", "standard")
            .putList("index.analysis.analyzer.search_a.filter", "lowercase", "syn")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn_localnames.txt")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.search_b.tokenizer", "standard")
            .putList("index.analysis.analyzer.search_b.filter", "lowercase", "syn")
            .build();

        IndexAnalyzers ia = build(sA);
        IndexAnalyzers ib = build(sB);

        NamedAnalyzer naA = ia.get("search_a");
        NamedAnalyzer naB = ib.get("search_b");
        assertNotNull("search_a must be found in index A", naA);
        assertNotNull("search_b must be found in index B", naB);
        // Same recipe → same underlying ReloadableCustomAnalyzer.
        assertSame("same recipe must share the underlying analyzer", naA.analyzer(), naB.analyzer());

        // Confirm baseline: "mp3-player" is not yet a synonym.
        List<String> tokensBefore = tokens(naA, "i-pod");
        assertFalse("mp3-player must not be a synonym before reload, got: " + tokensBefore, tokensBefore.contains("mp3-player"));

        // Update the file so reload would add "mp3-player" as a synonym for "i-pod".
        Files.writeString(file, "i-pod, ipod, mp3-player\n");

        // Simulate the order that triggers the bug: B's shard processes first, then A's,
        // both within the same reload request (same token).
        Object token = new Object();
        ib.reload(registry, settingsFor(sB), null, false, token);
        ia.reload(registry, settingsFor(sA), null, false, token);

        // After reload, querying "i-pod" must expand to include "mp3-player".
        // If the bug is present the components are never updated and this fails,
        // showing the stale token list (e.g. [i-pod, ipod]) rather than the new one.
        List<String> tokensAfter = tokens(naA, "i-pod");
        assertTrue(
            "after reload 'mp3-player' must be a synonym for 'i-pod' — got: " + tokensAfter
                + " (bug: B claimed the reload token but found null settings for 'search_a',"
                + " so reload was silently skipped for the whole broadcast)",
            tokensAfter.contains("mp3-player")
        );
    }

    /** Changing only the tokenizer of an otherwise-identical chain changes the recipe. */
    public void testDifferentTokenizerInSameChainDoesNotShare() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "whitespace")
            .putList("index.analysis.analyzer.a.filter", "lowercase")
            .build();
        assertNotSame(build(sA).get("a"), build(sB).get("a"));
    }

    /**
     * The mix that the per-factory contract cannot reach: changing the setting of one factory inside
     * a multi-factory analyzer must propagate to the analyzer's key. Here only the {@code stop}
     * filter's word list differs between two {@code [stop, lowercase]} chains.
     */
    public void testSettingChangeOnOneFilterInMultiFilterChainDoesNotShare() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.filter.my_stop.type", "stop")
            .putList("index.analysis.filter.my_stop.stopwords", "the")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "my_stop", "lowercase")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.filter.my_stop.type", "stop")
            .putList("index.analysis.filter.my_stop.stopwords", "and")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "my_stop", "lowercase")
            .build();
        assertNotSame(build(sA).get("a"), build(sB).get("a"));
    }

    // ---- Filters that reference other filters by name must not let analyzers share ----

    /**
     * The multiplexer's referenced sub-filters are resolved by name and need NOT appear in the
     * analyzer's own filter chain, so they are not folded into the analyzer's cache key. The
     * multiplexer therefore keys on identity: two indices whose referenced filter differs must not
     * share — otherwise the second would silently inherit the first's filter configuration.
     */
    public void testMultiplexerDoesNotShareWhenReferencedFilterDiffers() throws IOException {
        Settings sA = Settings.builder()
            .put("index.analysis.filter.inner.type", "ngram")
            .put("index.analysis.filter.inner.min_gram", 2)
            .put("index.analysis.filter.inner.max_gram", 3)
            .put("index.analysis.filter.mp.type", "multiplexer")
            .putList("index.analysis.filter.mp.filters", "inner")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "mp")
            .build();
        Settings sB = Settings.builder()
            .put("index.analysis.filter.inner.type", "ngram")
            .put("index.analysis.filter.inner.min_gram", 3)
            .put("index.analysis.filter.inner.max_gram", 4)
            .put("index.analysis.filter.mp.type", "multiplexer")
            .putList("index.analysis.filter.mp.filters", "inner")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "mp")
            .build();
        assertNotSame(
            "multiplexer-bearing analyzers must not share: the referenced filter is not in the chain key",
            build(sA).get("a"),
            build(sB).get("a")
        );
    }

    // ---- Default-sharingKey contract: explicit verification on real factories ----

    public void testStandardTokenizerFactoryFromRegistryShares() throws IOException {
        Settings sA = Settings.builder().put("index.analysis.tokenizer.t.type", "standard").build();
        Settings sB = Settings.builder().put("index.analysis.tokenizer.t.type", "standard").build();
        IndexAnalyzers iaA = build(Settings.builder().put(sA).put("index.analysis.analyzer.a.tokenizer", "t").build());
        IndexAnalyzers iaB = build(Settings.builder().put(sB).put("index.analysis.analyzer.a.tokenizer", "t").build());
        assertSame(iaA.get("a"), iaB.get("a"));
    }

    /**
     * Reload semantics for shared analyzers: when index A reloads a synonym chain that index B
     * also references, the shared {@link org.elasticsearch.index.analysis.ReloadableCustomAnalyzer}
     * is mutated in place — both A's and B's mapping references (captured in
     * {@link org.elasticsearch.index.mapper.TextSearchInfo} at mapping-build time) observe the
     * refreshed components. After both indices close the shared cache entry drains.
     *
     * <p>This is a deliberate trade-off: the pre-sharing per-index reload contract is broken for
     * shared analyzers (reload on A also refreshes B). The alternative (fork-on-reload) would
     * leave the mapping's cached {@code TextSearchInfo} pointing at the old wrapper, making the
     * refresh invisible to queries. Synonym-set updates are inherently cluster-global, so
     * propagating refreshes to all sharers matches user expectations.
     */
    public void testReloadAffectsAllSharersInPlaceAndDrainsOnClose() throws IOException {
        Path file = configDir.resolve("syn_reload.txt");
        Files.writeString(file, "i-pod, ipod\n");
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn_reload.txt")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        IndexSettings settingsA = IndexSettingsModule.newIndexSettings(
            "indexA",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).put(s).build()
        );
        IndexSettings settingsB = IndexSettingsModule.newIndexSettings(
            "indexB",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).put(s).build()
        );

        IndexAnalyzers ia = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, settingsA);
        IndexAnalyzers ib = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, settingsB);
        assertSame("preconditions: both indices share the same reloadable analyzer", ia.get("a"), ib.get("a"));

        NamedAnalyzer sharedBefore = ia.get("a");
        ReloadableCustomAnalyzer reloadable = (ReloadableCustomAnalyzer) sharedBefore.analyzer();
        AnalyzerComponents componentsBefore = reloadable.getComponents();

        // Edit the synonyms file so the reload picks up different content.
        Files.writeString(file, "i-pod, ipod, mp3-player\n");

        // Reload on A only. null resource means "reload every reloadable analyzer" (the
        // resource-name filter is for narrowing to a specific file or synonyms-set).
        ia.reload(registry, settingsA, null, false, null);

        // After reload: identity is preserved (in-place mutation), but the underlying
        // components have been swapped — observable to both A and B via their shared reference,
        // and to anything else that captured a reference at mapping-build time.
        assertSame("reload mutates in place, so A and B keep the same shared wrapper", sharedBefore, ia.get("a"));
        assertSame("B still references the same wrapper", sharedBefore, ib.get("a"));
        assertNotSame(
            "reload must produce fresh components even though the wrapper is the same instance",
            componentsBefore,
            reloadable.getComponents()
        );

        ia.close();
        ib.close();
        assertEquals("after closing both indices the cache must be empty", 0, registry.analyzerCacheSize());
    }

    /**
     * Three indices all reload the same recipe; closing all of them must drain the cache.
     * With in-place reload the wrapper identity is stable across all three reloads — they
     * continue to share the same instance throughout.
     */
    public void testReloadAcrossAllSharersDrainsOnClose() throws IOException {
        Path file = configDir.resolve("syn_coal.txt");
        Files.writeString(file, "i-pod, ipod\n");
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn_coal.txt")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        IndexSettings settingsBase = IndexSettingsModule.newIndexSettings(
            "indexA",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).put(s).build()
        );

        IndexAnalyzers a = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, settingsBase);
        IndexAnalyzers b = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, settingsBase);
        IndexAnalyzers c = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, settingsBase);

        // Sequentially-issued reloads (mimicking a broadcast) should coalesce to one new instance.
        a.reload(registry, settingsBase, null, false, null);
        b.reload(registry, settingsBase, null, false, null);
        c.reload(registry, settingsBase, null, false, null);

        assertSame("coalesced reload — A and B share the same new instance", a.get("a"), b.get("a"));
        assertSame("coalesced reload — B and C share the same new instance", b.get("a"), c.get("a"));

        a.close();
        b.close();
        c.close();

        assertEquals("after closing every sharer the cache must drain", 0, registry.analyzerCacheSize());
    }

    /**
     * Version-sensitive providers (Persian, Romanian, Unique) MUST encode their version-derived
     * state in their own {@code sharingKey()} since the composition key does
     * not carry {@link IndexVersion}. Otherwise mixed-version nodes would silently share
     * analyzers whose behavior actually differs.
     */
    public void testRomanianAnalyzerDoesNotShareAcrossVersions() throws IOException {
        Settings s = Settings.builder().put("index.analysis.analyzer.a.type", "romanian").build();
        IndexSettings current = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).put(s).build()
        );
        IndexSettings legacy = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersions.V_7_17_0).put(s).build()
        );
        IndexAnalyzers a = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, current);
        IndexAnalyzers b = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, legacy);
        try {
            assertNotSame("Romanian analyzer behavior differs by IndexVersion — must not share", a.get("a"), b.get("a"));
        } finally {
            a.close();
            b.close();
        }
    }

    /**
     * Per-index validation (here {@code index.max_ngram_diff}) MUST fire per-index even if
     * another index has already cached a compatible analyzer chain. The validation runs inside
     * the token-filter factory's constructor, which is called fresh per index in
     * {@code buildTokenFilterFactories} — before the analyzer cache is consulted — so a too-strict
     * limit on index B trips during factory construction and never reaches a cache hit.
     */
    public void testPerIndexNgramDiffValidationFiresEvenWithCachedSibling() throws IOException {
        Settings recipe = Settings.builder()
            .put("index.analysis.filter.ng.type", "ngram")
            .put("index.analysis.filter.ng.min_gram", 1)
            .put("index.analysis.filter.ng.max_gram", 5)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "ng")
            .build();
        // Index A allows the full diff (4 = max_gram - min_gram).
        Settings lenientNode = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.max_ngram_diff", 10)
            .put(recipe)
            .build();
        // Index B forbids it (max_ngram_diff = 1 < 4).
        Settings strictNode = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.max_ngram_diff", 1)
            .put(recipe)
            .build();

        // A builds successfully; the resulting analyzer chain is in the cache.
        IndexAnalyzers a = registry.build(
            IndexService.IndexCreationContext.CREATE_INDEX,
            IndexSettingsModule.newIndexSettings("a", lenientNode)
        );
        try {
            // B's factory constructor MUST throw — even though the recipe is identical and the
            // analyzer is already cached, B's per-index limit is enforced in the factory's own
            // constructor (called per-index from buildTokenFilterFactories), which runs BEFORE
            // produceAnalyzer ever consults the cache.
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> registry.build(IndexService.IndexCreationContext.CREATE_INDEX, IndexSettingsModule.newIndexSettings("b", strictNode))
            );
            assertThat(e.getMessage(), containsString("max_gram and min_gram"));
        } finally {
            a.close();
        }
    }

    /**
     * Two indices with the SAME inline synonym chain must produce the SAME tokens AND share
     * the analyzer. Two indices with DIFFERENT inline synonym chains must produce DIFFERENT
     * tokens AND NOT share. This is the end-to-end isolation guarantee — a sharing key
     * regression that mapped distinct recipes onto the same cache slot would silently corrupt
     * tokenization across indices.
     */
    public void testDifferentSynonymRulesProduceDifferentTokensAndDoNotShare() throws IOException {
        Settings recipeA = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms", "quick, fast")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        Settings recipeB = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms", "lazy, sleepy")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        IndexAnalyzers a = build(recipeA);
        IndexAnalyzers b = build(recipeB);
        try {
            assertNotSame("different synonym recipes must not share the analyzer", a.get("a"), b.get("a"));
            List<String> tokensA = tokens(a.get("a"), "the quick fox");
            List<String> tokensB = tokens(b.get("a"), "the quick fox");
            // A maps "quick" -> {quick, fast}; B doesn't expand "quick".
            assertTrue("A should expand 'quick' to include 'fast', got " + tokensA, tokensA.contains("fast"));
            assertFalse("B should NOT expand 'quick' to 'fast', got " + tokensB, tokensB.contains("fast"));
        } finally {
            a.close();
            b.close();
        }
    }

    /**
     * Sharing is byte-correct: two indices that DO share an analyzer (identical synonym recipe)
     * must produce identical token sequences for identical input. Guards against a wrapper
     * that mis-routes thread-local state.
     */
    public void testIdenticalSynonymRulesShareAndTokenizeIdentically() throws IOException {
        Settings recipe = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms", "quick, fast", "lazy, sleepy")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        IndexAnalyzers a = build(recipe);
        IndexAnalyzers b = build(recipe);
        try {
            assertSame("identical recipes must share", a.get("a"), b.get("a"));
            assertEquals(tokens(a.get("a"), "quick brown lazy fox"), tokens(b.get("a"), "quick brown lazy fox"));
        } finally {
            a.close();
            b.close();
        }
    }

    private static List<String> tokens(Analyzer analyzer, String input) throws IOException {
        List<String> out = new ArrayList<>();
        try (TokenStream ts = analyzer.tokenStream("f", input)) {
            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                out.add(term.toString());
            }
            ts.end();
        }
        return out;
    }

    /**
     * Lenient flag is per-factory, baked into the sharing key. Two indices with the SAME synonym
     * recipe but DIFFERENT lenient values must NOT share — each builds its own analyzer with its
     * own lenient policy, so a malformed rule that the lenient index would skip cannot
     * accidentally end up in the strict index's analyzer (or vice-versa).
     */
    public void testLenientFlagDifferentiatesSharingKey() throws IOException {
        Path file = configDir.resolve("syn_lenient.txt");
        Files.writeString(file, "i-pod, ipod\n");
        Settings sStrict = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn_lenient.txt")
            .put("index.analysis.filter.syn.lenient", false)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        Settings sLenient = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn_lenient.txt")
            .put("index.analysis.filter.syn.lenient", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        assertNotSame(build(sStrict).get("a"), build(sLenient).get("a"));
    }

    /**
     * Synonym build failure with {@code lenient=true} must not propagate as a shard-startup
     * failure — the existing factory contract returns an empty synonym map and proceeds. The
     * cache's single-flight path must not swallow that semantic: the cached entry should be a
     * usable analyzer (built with the empty synonym map), not a failed future.
     *
     * <p>This is asserted indirectly: with a malformed inline rule and {@code lenient=true},
     * the build path inside the factory swallows the parse error per-rule (see
     * {@code ESSolrSynonymParser.analyze}), and {@code buildSynonyms} returns a valid (possibly
     * empty) map. The resulting analyzer is then interned and shared.
     */
    public void testLenientSwallowsBadSynonymRulesUnderSharing() throws IOException {
        // A malformed pair triggers the lenient path inside the parser — without lenient it
        // would throw; with lenient=true it's silently skipped.
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms", "$$$,    ", "valid, good")
            .put("index.analysis.filter.syn.lenient", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        IndexAnalyzers a = build(s);
        IndexAnalyzers b = build(s);
        // Both builds must succeed (lenient absorbs the malformed rule); they must share, since
        // the recipe (including lenient=true) is identical.
        assertSame("lenient sharing precondition: same recipe shares", a.get("a"), b.get("a"));
        // And tokenization works — the valid synonym is in effect.
        List<String> ts = tokens(a.get("a"), "valid");
        assertTrue("lenient analyzer still applies valid synonym rules, got " + ts, ts.contains("good"));
    }

    /**
     * Structural-separation guarantee that underpins in-place reload's safety: a reloadable
     * (i.e. {@code updateable=true}) synonym filter forces the containing analyzer to
     * {@link org.elasticsearch.index.analysis.AnalysisMode#SEARCH_TIME}. The mapping layer
     * (see {@code TextParams}) refuses to use a SEARCH_TIME-only analyzer as the index-time
     * {@code analyzer} on a {@code text} field — so reload can only ever change query-time
     * tokenization, never the indexed data. This test exercises the end-to-end refusal so the
     * guarantee can't silently regress.
     */
    public void testReloadableAnalyzerCannotBeUsedAsIndexTimeAnalyzer() throws IOException {
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms", "quick, fast")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.reloadable_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.reloadable_analyzer.filter", "lowercase", "syn")
            .build();
        IndexAnalyzers ia = build(s);
        NamedAnalyzer reloadable = ia.get("reloadable_analyzer");
        assertNotNull("precondition: the reloadable analyzer is registered", reloadable);
        assertEquals("precondition: the analyzer is SEARCH_TIME-only", AnalysisMode.SEARCH_TIME, reloadable.getAnalysisMode());
        // Mapping-layer guard: using this analyzer as the index-time analyzer of a text field
        // throws — proves reload can never touch indexed data on any sharer.
        MapperException e = expectThrows(MapperException.class, () -> reloadable.checkAllowedInMode(AnalysisMode.INDEX_TIME));
        assertThat(e.getMessage(), containsString("not allowed to run in"));
    }

    /**
     * Synonyms-API auto-reload is resource-targeted: a reload triggered for synonym set
     * {@code foo} only refreshes analyzers that include {@code foo} in their resources. Indices
     * whose reloadable analyzer references a *different* set are skipped — even in the same
     * node, even sharing the cache.
     *
     * <p>This pins the contract that makes
     * {@code SynonymsManagementAPIService.reloadAnalyzers(synonymSetId, ...)} safe: when you
     * update synonym set {@code foo}, indices using set {@code bar} keep their pre-existing
     * components, no spurious work, no spurious cluster-state churn.
     */
    public void testResourceTargetedReloadOnlyAffectsMatchingAnalyzers() throws IOException {
        Settings recipeUsingFoo = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms_set", "foo")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        Settings recipeUsingBar = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms_set", "bar")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        IndexAnalyzers usingFoo = build(recipeUsingFoo);
        IndexAnalyzers usingBar = build(recipeUsingBar);

        ReloadableCustomAnalyzer rFoo = (ReloadableCustomAnalyzer) usingFoo.get("a").analyzer();
        ReloadableCustomAnalyzer rBar = (ReloadableCustomAnalyzer) usingBar.get("a").analyzer();

        // The two indices end up with distinct analyzers (different synonym sets → different
        // sharing keys), each carrying its own resource set.
        assertNotSame("different synonym sets must not share the analyzer", rFoo, rBar);
        assertTrue("foo analyzer reports the foo resource", rFoo.usesResource("foo"));
        assertFalse("foo analyzer must NOT match resource bar", rFoo.usesResource("bar"));
        assertTrue("bar analyzer reports the bar resource", rBar.usesResource("bar"));
        assertFalse("bar analyzer must NOT match resource foo", rBar.usesResource("foo"));

        // Concrete API simulation: reload triggered for set "foo" only affects indices using "foo".
        // The index using "bar" is skipped at the filter step in IndexAnalyzers.reload — its
        // components remain identical, the cache entry untouched.
        AnalyzerComponents barComponentsBefore = rBar.getComponents();
        usingBar.reload(registry, settingsFor(recipeUsingBar), "foo", false, null);
        assertSame(
            "reload for synonym set 'foo' must not refresh the analyzer that depends on 'bar'",
            barComponentsBefore,
            rBar.getComponents()
        );
    }

    private IndexSettings settingsFor(Settings recipe) {
        return IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).put(recipe).build()
        );
    }

    /**
     * Reload always re-reads the resource — there is no no-op short-circuit. An updateable synonym
     * filter's sharing key is content-independent (one shared live instance per recipe, refreshed
     * in place for every sharer), so there is no stamp to compare against and a redundant
     * {@code _reload_search_analyzers} rebuilds the components rather than skipping. Verified by
     * checking the components reference changes across a second, no-change reload.
     */
    public void testRedundantReloadStillRebuilds() throws IOException {
        Path file = configDir.resolve("syn_unchanged.txt");
        Files.writeString(file, "i-pod, ipod\n");
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn_unchanged.txt")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        IndexAnalyzers ia = build(s);
        ReloadableCustomAnalyzer rca = (ReloadableCustomAnalyzer) ia.get("a").analyzer();

        ia.reload(registry, settingsFor(s), null, false, null);
        AnalyzerComponents componentsAfterFirstReload = rca.getComponents();

        // Second reload without touching the file still rebuilds: reload unconditionally re-reads.
        ia.reload(registry, settingsFor(s), null, false, null);
        assertNotSame("reload always re-reads; there is no no-op skip", componentsAfterFirstReload, rca.getComponents());
    }

    /**
     * The first reload after index creation must always proceed even when no resource has
     * "changed" — for INDEX-source synonyms, CREATE_INDEX context loads a fake empty SynonymMap
     * (intentional master-thread protection), and the first reload is what actually loads the
     * real data. Skipping it would leave the analyzer permanently empty. Reload unconditionally
     * re-reads, so this is covered by asserting that the components instance is replaced.
     */
    public void testFirstReloadAfterCreateIndexAlwaysRuns() throws IOException {
        Path file = configDir.resolve("syn_first.txt");
        Files.writeString(file, "i-pod, ipod\n");
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn_first.txt")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        IndexAnalyzers ia = build(s);
        ReloadableCustomAnalyzer rca = (ReloadableCustomAnalyzer) ia.get("a").analyzer();
        AnalyzerComponents componentsAtCreateIndex = rca.getComponents();

        // First reload must always run: reload unconditionally re-reads, and this is the run that
        // loads the real synonyms data after the empty CREATE_INDEX stub.
        ia.reload(registry, settingsFor(s), null, false, null);
        assertNotSame(
            "first reload from CREATE_INDEX must load real data, never short-circuit",
            componentsAtCreateIndex,
            rca.getComponents()
        );
    }

    /**
     * When the file content changes, a reload produces fresh components carrying the new rules.
     */
    public void testReloadRunsWhenFileResourceChanged() throws IOException {
        Path file = configDir.resolve("syn_change.txt");
        Files.writeString(file, "i-pod, ipod\n");
        Settings s = Settings.builder()
            .put("index.analysis.filter.syn.type", "synonym")
            .put("index.analysis.filter.syn.synonyms_path", "syn_change.txt")
            .put("index.analysis.filter.syn.updateable", true)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "syn")
            .build();
        IndexAnalyzers ia = build(s);
        ReloadableCustomAnalyzer rca = (ReloadableCustomAnalyzer) ia.get("a").analyzer();
        AnalyzerComponents componentsBefore = rca.getComponents();

        Files.writeString(file, "i-pod, ipod, mp3-player\n");

        ia.reload(registry, settingsFor(s), null, false, null);
        assertNotSame("reload must produce fresh components carrying the new rules", componentsBefore, rca.getComponents());
    }

    public void testFactoryRegistrationsHaveSharingKey() throws IOException {
        // Build a registry that loads CommonAnalysisPlugin and verify a few representative
        // factories yield non-null sharingKey() results. (Default null impossible — interface
        // default returns this.)
        Settings s = Settings.builder()
            .put("index.analysis.filter.f.type", "lowercase")
            .put("index.analysis.tokenizer.tk.type", "standard")
            .put("index.analysis.char_filter.cf.type", "html_strip")
            .build();
        IndexSettings is = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).put(s).build()
        );
        Map<String, TokenFilterFactory> filters = registry.buildTokenFilterFactories(is);
        Map<String, TokenizerFactory> tokenizers = registry.buildTokenizerFactories(is);
        Map<String, CharFilterFactory> charFilters = registry.buildCharFilterFactories(is);
        assertNotNull(filters.get("f").sharingKey());
        assertNotNull(tokenizers.get("tk").sharingKey());
        assertNotNull(charFilters.get("cf").sharingKey());
    }
}
