/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;
import static org.hamcrest.Matchers.containsString;

public class ScriptedConditionTokenFilterTests extends ESTokenStreamTestCase {
    private TestThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void shutdownThreadPool() {
        threadPool.shutdownNow();
    }

    public void testSimpleCondition() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.analysis.filter.cond.type", "condition")
            .put("index.analysis.filter.cond.script.source", "token.getPosition() > 1")
            .putList("index.analysis.filter.cond.filter", "uppercase")
            .put("index.analysis.analyzer.myAnalyzer.type", "custom")
            .put("index.analysis.analyzer.myAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.myAnalyzer.filter", "cond")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        AnalysisPredicateScript.Factory factory = () -> new AnalysisPredicateScript() {
            @Override
            public boolean execute(Token token) {
                return token.getPosition() > 1;
            }
        };

        @SuppressWarnings("unchecked")
        ScriptService scriptService = new ScriptService(
            indexSettings,
            Collections.emptyMap(),
            Collections.emptyMap(),
            () -> 1L,
            TestProjectResolvers.singleProject(randomProjectIdOrDefault())
        ) {
            @Override
            public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
                assertEquals(context, AnalysisPredicateScript.CONTEXT);
                assertEquals(new Script("token.getPosition() > 1"), script);
                return (FactoryType) factory;
            }
        };

        CommonAnalysisPlugin plugin = new TestCommonAnalysisPluginBuilder(threadPool).scriptService(scriptService).build();
        AnalysisModule module = new AnalysisModule(
            TestEnvironment.newEnvironment(settings),
            Collections.singletonList(plugin),
            new StablePluginsRegistry()
        );

        IndexAnalyzers analyzers = module.getAnalysisRegistry().build(IndexCreationContext.CREATE_INDEX, idxSettings);

        try (NamedAnalyzer analyzer = analyzers.get("myAnalyzer")) {
            assertNotNull(analyzer);
            assertAnalyzesTo(analyzer, "Vorsprung Durch Technik", new String[] { "Vorsprung", "Durch", "TECHNIK" });
        }
    }

    /**
     * A condition wrapping an updateable filter inherits its search-time mode, so the analyzer is reloadable and refused at
     * index time. Indices created before that change keep the old behavior so their mappings still validate.
     */
    public void testWrappedUpdateableFilterModeIsGatedOnIndexVersion() throws Exception {
        IndexVersion before = IndexVersionUtils.getPreviousVersion(IndexVersions.CONDITION_FILTER_INHERITS_ANALYSIS_MODE);
        for (IndexVersion version : List.of(IndexVersion.current(), before)) {
            Settings indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                .put("index.analysis.filter.syn.type", "synonym")
                .putList("index.analysis.filter.syn.synonyms", "foo => bar")
                .put("index.analysis.filter.syn.updateable", true)
                .put("index.analysis.filter.cond.type", "condition")
                .put("index.analysis.filter.cond.script.source", "true")
                .putList("index.analysis.filter.cond.filter", "syn")
                .put("index.analysis.analyzer.a.tokenizer", "standard")
                .putList("index.analysis.analyzer.a.filter", "cond")
                .build();
            try (IndexAnalyzers analyzers = buildAnalyzers(indexSettings)) {
                AnalysisMode expected = version == before ? AnalysisMode.ALL : AnalysisMode.SEARCH_TIME;
                assertEquals("index version " + version, expected, analyzers.get("a").getAnalysisMode());
            }
        }
    }

    /** Builds an index's analyzers with every condition script always true. */
    private IndexAnalyzers buildAnalyzers(Settings indexSettings) throws IOException {
        CommonAnalysisPlugin plugin = new TestCommonAnalysisPluginBuilder(threadPool).scriptService(alwaysTrueScripts(indexSettings))
            .build();
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        AnalysisModule module = new AnalysisModule(
            TestEnvironment.newEnvironment(nodeSettings),
            List.of(plugin),
            new StablePluginsRegistry()
        );
        return module.getAnalysisRegistry()
            .build(IndexCreationContext.CREATE_INDEX, IndexSettingsModule.newIndexSettings("index", indexSettings));
    }

    private static ScriptService alwaysTrueScripts(Settings settings) {
        AnalysisPredicateScript.Factory alwaysTrue = () -> new AnalysisPredicateScript() {
            @Override
            public boolean execute(Token token) {
                return true;
            }
        };
        return new ScriptService(
            settings,
            Collections.emptyMap(),
            Collections.emptyMap(),
            () -> 1L,
            TestProjectResolvers.singleProject(randomProjectIdOrDefault())
        ) {
            @Override
            @SuppressWarnings("unchecked")
            public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
                return (FactoryType) alwaysTrue;
            }
        };
    }

    /** Later synonym rules are parsed without applying synonyms wrapped in an earlier condition; older indices keep applying them. */
    public void testLaterSynonymRulesIgnoreSynonymsWrappedInCondition() throws Exception {
        IndexVersion before = IndexVersionUtils.getPreviousVersion(IndexVersions.CONDITION_FILTER_INHERITS_ANALYSIS_MODE);
        for (IndexVersion version : List.of(IndexVersion.current(), before)) {
            Settings indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                .put("index.analysis.filter.s1.type", "synonym")
                .putList("index.analysis.filter.s1.synonyms", "foo => bar")
                .put("index.analysis.filter.s2.type", "synonym")
                .putList("index.analysis.filter.s2.synonyms", "qux => foo")
                .put("index.analysis.filter.c1.type", "condition")
                .put("index.analysis.filter.c1.script.source", "true")
                .putList("index.analysis.filter.c1.filter", "s1")
                .put("index.analysis.filter.c2.type", "condition")
                .put("index.analysis.filter.c2.script.source", "true")
                .putList("index.analysis.filter.c2.filter", "s2")
                .put("index.analysis.analyzer.a.tokenizer", "standard")
                .putList("index.analysis.analyzer.a.filter", "c1", "c2")
                .build();
            try (IndexAnalyzers analyzers = buildAnalyzers(indexSettings)) {
                assertAnalyzesTo(analyzers.get("a"), "qux", new String[] { version == before ? "bar" : "foo" });
            }
        }
    }

    /** A condition wrapping a filter that cannot parse synonyms, such as a multiplexer, still builds ahead of a synonym filter. */
    public void testConditionOverMultiplexerBeforeSynonymFilterBuilds() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.analysis.filter.mux.type", "multiplexer")
            .putList("index.analysis.filter.mux.filters", "lowercase")
            .put("index.analysis.filter.c.type", "condition")
            .put("index.analysis.filter.c.script.source", "true")
            .putList("index.analysis.filter.c.filter", "mux")
            .put("index.analysis.filter.syn.type", "synonym")
            .putList("index.analysis.filter.syn.synonyms", "foo => bar")
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "c", "syn")
            .build();
        try (IndexAnalyzers analyzers = buildAnalyzers(indexSettings)) {
            assertAnalyzesTo(analyzers.get("a"), "Foo", new String[] { "Foo", "bar" });
        }
    }

    /** Older indices still build a condition wrapping index-only and search-only filters; newer ones reject it like a plain chain. */
    public void testMixedModesAreOnlyMergedForNewIndices() throws Exception {
        TokenFilterFactory indexOnly = modeFilter("i", AnalysisMode.INDEX_TIME);
        TokenFilterFactory searchOnly = modeFilter("s", AnalysisMode.SEARCH_TIME);
        Settings filterSettings = Settings.builder().put("script.source", "true").putList("filter", "i", "s").build();
        IndexVersion before = IndexVersionUtils.getPreviousVersion(IndexVersions.CONDITION_FILTER_INHERITS_ANALYSIS_MODE);
        for (IndexVersion version : List.of(IndexVersion.current(), before)) {
            Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
            ScriptedConditionTokenFilterFactory condition = new ScriptedConditionTokenFilterFactory(
                IndexSettingsModule.newIndexSettings("index", indexSettings),
                "c",
                filterSettings,
                alwaysTrueScripts(indexSettings)
            );
            if (version == before) {
                TokenFilterFactory built = condition.getChainAwareTokenFilterFactory(
                    IndexCreationContext.CREATE_INDEX,
                    null,
                    List.of(),
                    List.of(),
                    Map.of("i", indexOnly, "s", searchOnly)::get
                );
                assertEquals(AnalysisMode.ALL, built.getAnalysisMode());
            } else {
                expectThrows(
                    IllegalStateException.class,
                    () -> condition.getChainAwareTokenFilterFactory(
                        IndexCreationContext.CREATE_INDEX,
                        null,
                        List.of(),
                        List.of(),
                        Map.of("i", indexOnly, "s", searchOnly)::get
                    )
                );
            }
        }
    }

    private static TokenFilterFactory modeFilter(String name, AnalysisMode mode) {
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return tokenStream;
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return mode;
            }
        };
    }

    public void testSelfReferentialConditionIsRejected() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.analysis.filter.cond.type", "condition")
            .put("index.analysis.filter.cond.script.source", "token.getPosition() > 1")
            .putList("index.analysis.filter.cond.filter", "cond")
            .put("index.analysis.analyzer.myAnalyzer.type", "custom")
            .put("index.analysis.analyzer.myAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.myAnalyzer.filter", "cond")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);
        AnalysisPredicateScript.Factory factory = () -> new AnalysisPredicateScript() {
            @Override
            public boolean execute(Token token) {
                return token.getPosition() > 1;
            }
        };
        @SuppressWarnings("unchecked")
        ScriptService scriptService = new ScriptService(
            indexSettings,
            Collections.emptyMap(),
            Collections.emptyMap(),
            () -> 1L,
            TestProjectResolvers.singleProject(randomProjectIdOrDefault())
        ) {
            @Override
            public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
                return (FactoryType) factory;
            }
        };
        CommonAnalysisPlugin plugin = new TestCommonAnalysisPluginBuilder(threadPool).scriptService(scriptService).build();
        AnalysisModule module = new AnalysisModule(
            TestEnvironment.newEnvironment(settings),
            Collections.singletonList(plugin),
            new StablePluginsRegistry()
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> module.getAnalysisRegistry().build(IndexCreationContext.CREATE_INDEX, idxSettings)
        );
        assertThat(e.getMessage(), containsString("Token filter [cond] refers to itself"));
    }
}
