/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.indices.analysis.lucene.AppendTokenFilter;
import org.elasticsearch.indices.analysis.lucene.CharSkippingTokenizer;
import org.elasticsearch.indices.analysis.lucene.ReplaceCharToNumber;
import org.elasticsearch.indices.analysis.lucene.SkipStartingWithDigitTokenFilter;
import org.elasticsearch.plugin.NamedComponent;
import org.elasticsearch.plugin.analysis.AnalysisMode;
import org.elasticsearch.plugin.analysis.AnalyzerFactory;
import org.elasticsearch.plugin.analysis.CharFilterFactory;
import org.elasticsearch.plugin.analysis.TokenFilterFactory;
import org.elasticsearch.plugin.analysis.TokenizerFactory;
import org.elasticsearch.plugins.scanners.NameToPluginInfo;
import org.elasticsearch.plugins.scanners.NamedComponentReader;
import org.elasticsearch.plugins.scanners.PluginInfo;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.Matchers.equalTo;

public class StableAnalysisPluginsNoSettingsTests extends ESTestCase {
    private final Settings emptyNodeSettings = Settings.builder()
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();

    public IndexAnalyzers getIndexAnalyzers(Settings settings) throws IOException {
        AnalysisRegistry registry = setupRegistry();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        return registry.build(idxSettings);
    }

    public void testStablePlugins() throws IOException {
        Version version = VersionUtils.randomVersion(random());
        IndexAnalyzers analyzers = getIndexAnalyzers(
            Settings.builder()
                .put("index.analysis.analyzer.char_filter_test.tokenizer", "standard")
                .put("index.analysis.analyzer.char_filter_test.char_filter", "stableCharFilterFactory")

                .put("index.analysis.analyzer.token_filter_test.tokenizer", "standard")
                .put("index.analysis.analyzer.token_filter_test.filter", "stableTokenFilterFactory")

                .put("index.analysis.analyzer.tokenizer_test.tokenizer", "stableTokenizerFactory")

                .put("index.analysis.analyzer.analyzer_provider_test.type", "stableAnalyzerFactory")

                .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                .build()
        );
        assertTokenStreamContents(analyzers.get("char_filter_test").tokenStream("", "t#st"), new String[] { "t3st" });
        assertTokenStreamContents(
            analyzers.get("token_filter_test").tokenStream("", "1test 2test 1test 3test "),
            new String[] { "2test", "3test" }
        );
        assertTokenStreamContents(analyzers.get("tokenizer_test").tokenStream("", "x_y_z"), new String[] { "x", "y", "z" });
        assertTokenStreamContents(analyzers.get("analyzer_provider_test").tokenStream("", "1x_y_#z"), new String[] { "y", "3z" });

        assertThat(analyzers.get("char_filter_test").normalize("", "t#st").utf8ToString(), equalTo("t3st"));
        assertThat(
            analyzers.get("token_filter_test").normalize("", "1test 2test 1test 3test ").utf8ToString(),
            equalTo("1test 2test 1test 3test 1")
        );
    }

    @NamedComponent("stableCharFilterFactory")
    public static class TestCharFilterFactory implements CharFilterFactory {

        @Override
        public Reader create(Reader reader) {
            return new ReplaceCharToNumber(reader, "#", 3);
        }

        @Override
        public Reader normalize(Reader reader) {
            return new ReplaceCharToNumber(reader, "#", 3);
        }
    }

    @NamedComponent("stableTokenFilterFactory")
    public static class TestTokenFilterFactory implements TokenFilterFactory {

        @Override
        public TokenStream create(TokenStream tokenStream) {
            return new SkipStartingWithDigitTokenFilter(tokenStream, 1);
        }

        @Override
        public TokenStream normalize(TokenStream tokenStream) {
            return new AppendTokenFilter(tokenStream, "1");
        }

        @Override
        public AnalysisMode getAnalysisMode() {
            return TokenFilterFactory.super.getAnalysisMode();
        }

    }

    @NamedComponent("stableTokenizerFactory")
    public static class TestTokenizerFactory implements TokenizerFactory {

        @Override
        public Tokenizer create() {
            return new CharSkippingTokenizer(List.of("_"));
        }

    }

    @NamedComponent("stableAnalyzerFactory")
    public static class TestAnalyzerFactory implements AnalyzerFactory {

        @Override
        public Analyzer create() {
            return new CustomAnalyzer();
        }

        static class CustomAnalyzer extends Analyzer {

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                var tokenizer = new CharSkippingTokenizer(List.of("_"));
                var tokenFilter = new SkipStartingWithDigitTokenFilter(tokenizer, 1);
                return new TokenStreamComponents(r -> tokenizer.setReader(new ReplaceCharToNumber(r, "#", 3)), tokenFilter);
            }
        }
    }

    private AnalysisRegistry setupRegistry() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        AnalysisRegistry registry = new AnalysisModule(
            TestEnvironment.newEnvironment(emptyNodeSettings),
            emptyList(),
            new StablePluginsRegistry(
                new NamedComponentReader(),
                Map.of(
                    CharFilterFactory.class.getCanonicalName(),
                    new NameToPluginInfo(
                        Map.of(
                            "stableCharFilterFactory",
                            new PluginInfo("stableCharFilterFactory", TestCharFilterFactory.class.getName(), classLoader)
                        )
                    ),
                    TokenFilterFactory.class.getCanonicalName(),
                    new NameToPluginInfo(
                        Map.of(
                            "stableTokenFilterFactory",
                            new PluginInfo("stableTokenFilterFactory", TestTokenFilterFactory.class.getName(), classLoader)
                        )
                    ),
                    TokenizerFactory.class.getCanonicalName(),
                    new NameToPluginInfo(
                        Map.of(
                            "stableTokenizerFactory",
                            new PluginInfo("stableTokenizerFactory", TestTokenizerFactory.class.getName(), classLoader)
                        )
                    ),
                    AnalyzerFactory.class.getCanonicalName(),
                    new NameToPluginInfo(
                        Map.of(
                            "stableAnalyzerFactory",
                            new PluginInfo("stableAnalyzerFactory", TestAnalyzerFactory.class.getName(), classLoader)
                        )
                    )
                )
            )
        ).getAnalysisRegistry();
        return registry;
    }
}
