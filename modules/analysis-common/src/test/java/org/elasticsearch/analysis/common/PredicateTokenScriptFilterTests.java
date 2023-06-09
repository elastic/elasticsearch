/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;

import java.io.IOException;
import java.util.Collections;

public class PredicateTokenScriptFilterTests extends ESTokenStreamTestCase {

    public void testSimpleFilter() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.filter.f.type", "predicate_token_filter")
            .put("index.analysis.filter.f.script.source", "my_script")
            .put("index.analysis.analyzer.myAnalyzer.type", "custom")
            .put("index.analysis.analyzer.myAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.myAnalyzer.filter", "f")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        AnalysisPredicateScript.Factory factory = () -> new AnalysisPredicateScript() {
            @Override
            public boolean execute(Token token) {
                return token.getPosition() < 2 || token.getPosition() > 4;
            }
        };

        @SuppressWarnings("unchecked")
        ScriptService scriptService = new ScriptService(indexSettings, Collections.emptyMap(), Collections.emptyMap(), () -> 1L) {
            @Override
            public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
                assertEquals(context, AnalysisPredicateScript.CONTEXT);
                assertEquals(new Script("my_script"), script);
                return (FactoryType) factory;
            }
        };
        Client client = new MockClient(Settings.EMPTY, null);
        CommonAnalysisPlugin plugin = new CommonAnalysisPlugin();
        plugin.createComponents(client, null, null, null, scriptService, null, null, null, null, null, null, Tracer.NOOP, null);
        AnalysisModule module = new AnalysisModule(
            TestEnvironment.newEnvironment(settings),
            Collections.singletonList(plugin),
            new StablePluginsRegistry()
        );

        IndexAnalyzers analyzers = module.getAnalysisRegistry().build(idxSettings);

        try (NamedAnalyzer analyzer = analyzers.get("myAnalyzer")) {
            assertNotNull(analyzer);
            assertAnalyzesTo(analyzer, "Oh what a wonderful thing to be", new String[] { "Oh", "what", "to", "be" });
        }

    }

    private class MockClient extends AbstractClient {
        MockClient(Settings settings, ThreadPool threadPool) {
            super(settings, threadPool);
        }

        @Override
        public void close() {}

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {}
    }

}
