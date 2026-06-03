/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;

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
}
