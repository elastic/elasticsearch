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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Analyzer-sharing cases in analysis-common that {@code CommonAnalysisFactoryTests}' settings contract
 * cannot express: keys that depend on the index created version, and per-index validation that must
 * still run when a sibling index already cached the same recipe. Builds real {@link IndexAnalyzers}
 * through {@link AnalysisRegistry}, the path index creation takes.
 */
public class SharingKeyTests extends ESTestCase {

    private TestThreadPool threadPool;
    private Path configDir;
    private AnalysisRegistry registry;
    private final List<IndexAnalyzers> tracked = new ArrayList<>();

    @Before
    public void setUpRegistry() throws IOException {
        assumeTrue(
            "analyzer sharing feature flag disabled (release build); instance propagation is not observable",
            AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled()
        );
        threadPool = new TestThreadPool(getTestName());
        Path home = createTempDir();
        configDir = Files.createDirectory(home.resolve("config"));
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        CommonAnalysisPlugin plugin = new TestCommonAnalysisPluginBuilder(threadPool).scriptService(newScriptService()).build();
        registry = new AnalysisModule(TestEnvironment.newEnvironment(nodeSettings), List.of(plugin), new StablePluginsRegistry())
            .getAnalysisRegistry();
    }

    @After
    public void closeAndAssertNoLeaks() throws IOException {
        if (registry == null) {
            return;
        }
        IOUtils.close(tracked);
        registry.assertNoCachedEntries();
        registry.close();
        threadPool.shutdownNow();
    }

    /**
     * Compiles every script to a new factory instance, as a script cache miss would, so sharing cannot rely
     * on two indices receiving the same compiled factory.
     */
    private static ScriptService newScriptService() {
        return new ScriptService(
            Settings.EMPTY,
            Map.of(),
            Map.of(),
            () -> 1L,
            TestProjectResolvers.singleProject(randomProjectIdOrDefault())
        ) {
            @Override
            @SuppressWarnings("unchecked")
            public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
                assertEquals(AnalysisPredicateScript.CONTEXT, context);
                String source = script.getIdOrCode();
                AnalysisPredicateScript.Factory factory = () -> new AnalysisPredicateScript() {
                    @Override
                    public boolean execute(Token token) {
                        return source.isEmpty() == false;
                    }
                };
                return (FactoryType) factory;
            }
        };
    }

    private Analyzer build(IndexVersion version, Settings analysis) throws IOException {
        Settings s = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).put(analysis).build();
        IndexAnalyzers ia = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, IndexSettingsModule.newIndexSettings("test", s));
        tracked.add(ia);
        // The per-index NamedAnalyzer wrapper is allocated per build, so compare the wrapped analyzer.
        return ia.get("a").analyzer();
    }

    /** Before Lucene 10 the romanian analyzer keeps cedilla forms, so the version fork must be part of the key. */
    public void testRomanianAnalyzerDoesNotShareAcrossVersions() throws IOException {
        IndexVersion legacy = IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0);
        assumeTrue("no read-compatible version predates Lucene 10", legacy.onOrAfter(IndexVersionUtils.getLowestReadCompatibleVersion()));
        Settings romanian = Settings.builder().put("index.analysis.analyzer.a.type", "romanian").build();

        Analyzer current = build(IndexVersion.current(), romanian);
        assertSame(current, build(IndexVersion.current(), romanian));
        assertNotSame("romanian analyzer behavior differs by index version and must not share", current, build(legacy, romanian));
    }

    /** The unique filter only corrects position increments on indices created after the fix, so that must be part of the key. */
    public void testUniqueFilterDoesNotShareAcrossPositionFixVersion() throws IOException {
        IndexVersion legacy = IndexVersionUtils.getPreviousVersion(IndexVersions.UNIQUE_TOKEN_FILTER_POS_FIX);
        assumeTrue("no read-compatible version predates the fix", legacy.onOrAfter(IndexVersionUtils.getLowestReadCompatibleVersion()));
        Settings unique = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "u")
            .put("index.analysis.filter.u.type", "unique")
            .build();

        Analyzer current = build(IndexVersion.current(), unique);
        assertSame(current, build(IndexVersion.current(), unique));
        assertNotSame("unique filter behavior differs by index version and must not share", current, build(legacy, unique));
    }

    /**
     * Per-index validation (here {@code index.max_ngram_diff}) must fire even when another index has
     * already cached an identical chain: the check runs in the factory constructor, which is called
     * per index before the analyzer cache is consulted.
     */
    public void testPerIndexNgramDiffValidationFiresEvenWithCachedSibling() throws IOException {
        Settings recipe = Settings.builder()
            .put("index.analysis.filter.ng.type", "ngram")
            .put("index.analysis.filter.ng.min_gram", 1)
            .put("index.analysis.filter.ng.max_gram", 5)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "ng")
            .build();
        build(IndexVersion.current(), Settings.builder().put("index.max_ngram_diff", 10).put(recipe).build());

        Settings strict = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.max_ngram_diff", 1)
            .put(recipe)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.build(IndexService.IndexCreationContext.CREATE_INDEX, IndexSettingsModule.newIndexSettings("b", strict))
        );
        assertThat(e.getMessage(), containsString("max_gram and min_gram"));
    }

    /** Since Lucene 10 the persian analyzer also stems, so the version fork must be part of the key. */
    public void testPersianAnalyzerDoesNotShareAcrossVersions() throws IOException {
        IndexVersion legacy = IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0);
        assumeTrue("no read-compatible version predates Lucene 10", legacy.onOrAfter(IndexVersionUtils.getLowestReadCompatibleVersion()));
        Settings persian = Settings.builder().put("index.analysis.analyzer.a.type", "persian").build();

        Analyzer current = build(IndexVersion.current(), persian);
        assertSame(current, build(IndexVersion.current(), persian));
        assertNotSame("persian analyzer behavior differs by index version and must not share", current, build(legacy, persian));
    }

    /** {@code keywords_pattern} cannot be combined with {@code keywords}, so the settings contract cannot vary it. */
    public void testKeywordMarkerPatternIsKeyed() throws IOException {
        Analyzer pattern = build(IndexVersion.current(), keywordMarker().put(KEYWORD_MARKER + "keywords_pattern", "fo+").build());
        assertSame(pattern, build(IndexVersion.current(), keywordMarker().put(KEYWORD_MARKER + "keywords_pattern", "fo+").build()));
        assertNotSame(pattern, build(IndexVersion.current(), keywordMarker().put(KEYWORD_MARKER + "keywords_pattern", "ba+").build()));
        assertNotSame(pattern, build(IndexVersion.current(), keywordMarker().putList(KEYWORD_MARKER + "keywords", "fo+").build()));
    }

    /**
     * An updateable keyword_marker re-reads {@code keywords_path} on recovery and reload, so the content read
     * at creation does not describe a shared instance for long. Those never share; inline keywords, which
     * only change with the index settings, still do.
     */
    public void testUpdateableKeywordMarkerFromFileDoesNotShare() throws IOException {
        Files.writeString(configDir.resolve("keywords.txt"), "foo\n");
        Settings fromFile = keywordMarker().put(KEYWORD_MARKER + "keywords_path", "keywords.txt").build();
        Settings updateableFromFile = Settings.builder().put(fromFile).put(KEYWORD_MARKER + "updateable", true).build();
        assertNotSame(build(IndexVersion.current(), updateableFromFile), build(IndexVersion.current(), updateableFromFile));
        assertSame(build(IndexVersion.current(), fromFile), build(IndexVersion.current(), fromFile));

        Settings updateableInline = keywordMarker().putList(KEYWORD_MARKER + "keywords", "foo")
            .put(KEYWORD_MARKER + "updateable", true)
            .build();
        assertSame(build(IndexVersion.current(), updateableInline), build(IndexVersion.current(), updateableInline));
    }

    private static final String KEYWORD_MARKER = "index.analysis.filter.k.";

    private static Settings.Builder keywordMarker() {
        return Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "k")
            .put(KEYWORD_MARKER + "type", "keyword_marker");
    }

    /**
     * The hyphenation patterns are read from a file at creation, so the key covers their content: two
     * indices pointing at the same path share only while the file is unchanged.
     */
    public void testHyphenationDecompounderKeysOnPatternsContent() throws IOException {
        Path patterns = configDir.resolve("patterns.xml");
        copyResource("de_DR.xml", patterns);
        Settings hyphenation = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "h")
            .put("index.analysis.filter.h.type", "hyphenation_decompounder")
            .put("index.analysis.filter.h.hyphenation_patterns_path", "patterns.xml")
            .putList("index.analysis.filter.h.word_list", "kaffee", "maschine")
            .build();

        Analyzer original = build(IndexVersion.current(), hyphenation);
        assertSame(original, build(IndexVersion.current(), hyphenation));

        String content = Files.readString(patterns);
        String changed = content.replaceFirst("</patterns>", "a1b\n</patterns>");
        assertNotEquals("test patterns file must contain a patterns element", content, changed);
        Files.writeString(patterns, changed);
        assertNotSame("changed hyphenation patterns must not share", original, build(IndexVersion.current(), hyphenation));
    }

    private void copyResource(String resource, Path target) throws IOException {
        try (InputStream in = getClass().getResourceAsStream(resource)) {
            assertNotNull("missing test resource " + resource, in);
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /** The predicate filter keys on the script, not on the compiled factory the script service returns. */
    public void testPredicateTokenFilterKeysOnScript() throws IOException {
        Analyzer predicate = build(IndexVersion.current(), predicateFilter("token.position > 1"));
        assertSame(predicate, build(IndexVersion.current(), predicateFilter("token.position > 1")));
        assertNotSame(predicate, build(IndexVersion.current(), predicateFilter("token.position > 2")));
    }

    private static Settings predicateFilter(String source) {
        return Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "p")
            .put("index.analysis.filter.p.type", "predicate_token_filter")
            .put("index.analysis.filter.p.script.source", source)
            .build();
    }

    /**
     * The condition filter names filters that are resolved per index and need not be in the analyzer's chain,
     * so the analyzer key does not cover them. Here {@code x} differs between the two recipes but the chain,
     * {@code [c]}, does not; the filter therefore never shares, even between identical recipes.
     */
    public void testConditionFilterNeverShares() throws IOException {
        Settings.Builder recipe = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "c")
            .put("index.analysis.filter.c.type", "condition")
            .put("index.analysis.filter.c.script.source", "token.position > 1")
            .putList("index.analysis.filter.c.filter", "x");
        Analyzer lowercase = build(
            IndexVersion.current(),
            Settings.builder().put(recipe.build()).put("index.analysis.filter.x.type", "lowercase").build()
        );
        Analyzer uppercase = build(
            IndexVersion.current(),
            Settings.builder().put(recipe.build()).put("index.analysis.filter.x.type", "uppercase").build()
        );
        assertNotSame("condition filters whose named filters differ must not share", lowercase, uppercase);
        Analyzer again = build(
            IndexVersion.current(),
            Settings.builder().put(recipe.build()).put("index.analysis.filter.x.type", "lowercase").build()
        );
        assertNotSame(lowercase, again);
    }
}
