/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.indices.analysis.lucene.AppendTokenFilter;
import org.elasticsearch.plugin.analysis.api.AnalysisMode;
import org.elasticsearch.plugin.api.NamedComponent;
import org.elasticsearch.plugins.scanners.NameToPluginInfo;
import org.elasticsearch.plugins.scanners.NamedComponentReader;
import org.elasticsearch.plugins.scanners.PluginInfo;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.io.Reader;
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
    public static class TestCharFilterFactory implements org.elasticsearch.plugin.analysis.api.CharFilterFactory {

        @Override
        public Reader create(Reader reader) {
            return new ReplaceHash(reader);
        }

        @Override
        public Reader normalize(Reader reader) {
            return new ReplaceHash(reader);
        }

    }

    static class ReplaceHash extends MappingCharFilter {

        ReplaceHash(Reader in) {
            super(charMap(), in);
        }

        private static NormalizeCharMap charMap() {
            NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
            builder.add("#", "3");
            return builder.build();
        }
    }

    @NamedComponent("stableTokenFilterFactory")
    public static class TestTokenFilterFactory implements org.elasticsearch.plugin.analysis.api.TokenFilterFactory {

        @Override
        public TokenStream create(TokenStream tokenStream) {

            return new Skip1TokenFilter(tokenStream);
        }

        @Override
        public TokenStream normalize(TokenStream tokenStream) {
            return new AppendTokenFilter(tokenStream, "1");
        }

        @Override
        public AnalysisMode getAnalysisMode() {
            return org.elasticsearch.plugin.analysis.api.TokenFilterFactory.super.getAnalysisMode();
        }

    }

    static class Skip1TokenFilter extends FilteringTokenFilter {

        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

        Skip1TokenFilter(TokenStream in) {
            super(in);
        }

        @Override
        protected boolean accept() throws IOException {
            return termAtt.buffer()[0] != '1';
        }
    }

    @NamedComponent("stableTokenizerFactory")
    public static class TestTokenizerFactory implements org.elasticsearch.plugin.analysis.api.TokenizerFactory {

        @Override
        public Tokenizer create() {
            return new UnderscoreTokenizer();
        }

    }

    static class UnderscoreTokenizer extends CharTokenizer {

        @Override
        protected boolean isTokenChar(int c) {
            return c != '_';
        }
    }

    @NamedComponent("stableAnalyzerFactory")
    public static class TestAnalyzerFactory implements org.elasticsearch.plugin.analysis.api.AnalyzerFactory {

        @Override
        public Analyzer create() {
            return new CustomAnalyzer();
        }

        static class CustomAnalyzer extends Analyzer {

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                var tokenizer = new UnderscoreTokenizer();
                var tokenFilter = new Skip1TokenFilter(tokenizer);
                return new TokenStreamComponents(r -> tokenizer.setReader(new ReplaceHash(r)), tokenFilter);
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
                    org.elasticsearch.plugin.analysis.api.CharFilterFactory.class.getCanonicalName(),
                    new NameToPluginInfo(
                        Map.of(
                            "stableCharFilterFactory",
                            new PluginInfo("stableCharFilterFactory", TestCharFilterFactory.class.getName(), classLoader)
                        )
                    ),
                    org.elasticsearch.plugin.analysis.api.TokenFilterFactory.class.getCanonicalName(),
                    new NameToPluginInfo(
                        Map.of(
                            "stableTokenFilterFactory",
                            new PluginInfo("stableTokenFilterFactory", TestTokenFilterFactory.class.getName(), classLoader)
                        )
                    ),
                    org.elasticsearch.plugin.analysis.api.TokenizerFactory.class.getCanonicalName(),
                    new NameToPluginInfo(
                        Map.of(
                            "stableTokenizerFactory",
                            new PluginInfo("stableTokenizerFactory", TestTokenizerFactory.class.getName(), classLoader)
                        )
                    ),
                    org.elasticsearch.plugin.analysis.api.AnalyzerFactory.class.getCanonicalName(),
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
