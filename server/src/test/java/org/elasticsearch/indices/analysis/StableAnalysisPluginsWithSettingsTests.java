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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.indices.analysis.lucene.AppendTokenFilter;
import org.elasticsearch.indices.analysis.lucene.CharSkippingTokenizer;
import org.elasticsearch.indices.analysis.lucene.ReplaceCharToNumber;
import org.elasticsearch.indices.analysis.lucene.SkipStartingWithDigitTokenFilter;
import org.elasticsearch.plugin.Inject;
import org.elasticsearch.plugin.NamedComponent;
import org.elasticsearch.plugin.analysis.AnalysisMode;
import org.elasticsearch.plugin.analysis.AnalyzerFactory;
import org.elasticsearch.plugin.analysis.CharFilterFactory;
import org.elasticsearch.plugin.analysis.TokenFilterFactory;
import org.elasticsearch.plugin.analysis.TokenizerFactory;
import org.elasticsearch.plugin.settings.AnalysisSettings;
import org.elasticsearch.plugin.settings.BooleanSetting;
import org.elasticsearch.plugin.settings.IntSetting;
import org.elasticsearch.plugin.settings.ListSetting;
import org.elasticsearch.plugin.settings.LongSetting;
import org.elasticsearch.plugin.settings.StringSetting;
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

public class StableAnalysisPluginsWithSettingsTests extends ESTestCase {

    protected final Settings emptyNodeSettings = Settings.builder()
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();

    public void testCharFilters() throws IOException {
        IndexAnalyzers analyzers = getIndexAnalyzers(
            Settings.builder()
                .put("index.analysis.char_filter.my_char_filter.type", "stableCharFilterFactory")
                .put("index.analysis.char_filter.my_char_filter.old_char", "#")
                .put("index.analysis.char_filter.my_char_filter.new_number", 3)

                .put("index.analysis.analyzer.char_filter_test.tokenizer", "standard")
                .put("index.analysis.analyzer.char_filter_test.char_filter", "my_char_filter")

                .put("index.analysis.analyzer.char_filter_with_defaults_test.tokenizer", "standard")
                .put("index.analysis.analyzer.char_filter_with_defaults_test.char_filter", "stableCharFilterFactory")

                .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
                .build()
        );
        assertTokenStreamContents(analyzers.get("char_filter_test").tokenStream("", "t#st"), new String[] { "t3st" });
        assertTokenStreamContents(analyzers.get("char_filter_with_defaults_test").tokenStream("", "t t"), new String[] { "t0t" });
        assertThat(analyzers.get("char_filter_test").normalize("", "t#st").utf8ToString(), equalTo("t3st"));
    }

    public void testTokenFilters() throws IOException {
        IndexAnalyzers analyzers = getIndexAnalyzers(
            Settings.builder()
                .put("index.analysis.filter.my_token_filter.type", "stableTokenFilterFactory")
                .put("index.analysis.filter.my_token_filter.token_filter_number", 1L)

                .put("index.analysis.analyzer.token_filter_test.tokenizer", "standard")
                .put("index.analysis.analyzer.token_filter_test.filter", "my_token_filter")
                .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
                .build()
        );
        assertTokenStreamContents(
            analyzers.get("token_filter_test").tokenStream("", "1test 2test 1test 3test "),
            new String[] { "2test", "3test" }
        );

        assertThat(
            analyzers.get("token_filter_test").normalize("", "1test 2test 1test 3test ").utf8ToString(),
            equalTo("1test 2test 1test 3test 1")
        );
    }

    public void testTokenizer() throws IOException {
        IndexAnalyzers analyzers = getIndexAnalyzers(
            Settings.builder()
                .put("index.analysis.tokenizer.my_tokenizer.type", "stableTokenizerFactory")
                .putList("index.analysis.tokenizer.my_tokenizer.tokenizer_list_of_chars", "_", " ")

                .put("index.analysis.analyzer.tokenizer_test.tokenizer", "my_tokenizer")
                .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
                .build()
        );
        assertTokenStreamContents(analyzers.get("tokenizer_test").tokenStream("", "x_y z"), new String[] { "x", "y", "z" });
    }

    public void testAnalyzer() throws IOException {
        IndexAnalyzers analyzers = getIndexAnalyzers(
            Settings.builder()
                .put("index.analysis.analyzer.analyzer_provider_test.type", "stableAnalyzerFactory")
                .putList("index.analysis.analyzer.analyzer_provider_test.tokenizer_list_of_chars", "_", " ")
                .put("index.analysis.analyzer.analyzer_provider_test.token_filter_number", 1L)
                .put("index.analysis.analyzer.analyzer_provider_test.old_char", "#")
                .put("index.analysis.analyzer.analyzer_provider_test.new_number", 3)
                .put("index.analysis.analyzer.analyzer_provider_test.analyzerUseTokenListOfChars", true)
                .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
                .build()
        );
        assertTokenStreamContents(analyzers.get("analyzer_provider_test").tokenStream("", "1x_y_#z"), new String[] { "y", "3z" });
    }

    protected IndexAnalyzers getIndexAnalyzers(Settings settings) throws IOException {
        AnalysisRegistry registry = setupRegistry();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        return registry.build(IndexCreationContext.CREATE_INDEX, idxSettings);
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

    @AnalysisSettings
    public interface TestAnalysisSettings {
        @StringSetting(path = "old_char", defaultValue = " ")
        String oldChar();

        @IntSetting(path = "new_number", defaultValue = 0)
        int newNumber();

        @LongSetting(path = "token_filter_number", defaultValue = 0L)
        long tokenFilterNumber();

        @ListSetting(path = "tokenizer_list_of_chars")
        java.util.List<String> tokenizerListOfChars();

        @BooleanSetting(path = "analyzerUseTokenListOfChars", defaultValue = false)
        boolean analyzerUseTokenListOfChars();
    }

    @NamedComponent("stableAnalyzerFactory")
    public static class TestAnalyzerFactory implements AnalyzerFactory {

        private final TestAnalysisSettings settings;

        @Inject
        public TestAnalyzerFactory(TestAnalysisSettings settings) {
            this.settings = settings;
        }

        @Override
        public Analyzer create() {
            return new CustomAnalyzer(settings);
        }

        static class CustomAnalyzer extends Analyzer {

            private final TestAnalysisSettings settings;

            CustomAnalyzer(TestAnalysisSettings settings) {
                this.settings = settings;
            }

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                var tokenizer = new CharSkippingTokenizer(settings.tokenizerListOfChars());
                long tokenFilterNumber = settings.analyzerUseTokenListOfChars() ? settings.tokenFilterNumber() : -1;
                var tokenFilter = new SkipStartingWithDigitTokenFilter(tokenizer, tokenFilterNumber);
                return new TokenStreamComponents(
                    r -> tokenizer.setReader(new ReplaceCharToNumber(r, settings.oldChar(), settings.newNumber())),
                    tokenFilter
                );
            }
        }
    }

    @NamedComponent("stableCharFilterFactory")
    public static class TestCharFilterFactory implements CharFilterFactory {
        private final String oldChar;
        private final int newNumber;

        @Inject
        public TestCharFilterFactory(TestAnalysisSettings settings) {
            oldChar = settings.oldChar();
            newNumber = settings.newNumber();
        }

        @Override
        public Reader create(Reader reader) {
            return new ReplaceCharToNumber(reader, oldChar, newNumber);
        }

        @Override
        public Reader normalize(Reader reader) {
            return new ReplaceCharToNumber(reader, oldChar, newNumber);
        }

    }

    @NamedComponent("stableTokenFilterFactory")
    public static class TestTokenFilterFactory implements TokenFilterFactory {

        private final long tokenFilterNumber;

        @Inject
        public TestTokenFilterFactory(TestAnalysisSettings settings) {
            this.tokenFilterNumber = settings.tokenFilterNumber();
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            return new SkipStartingWithDigitTokenFilter(tokenStream, tokenFilterNumber);
        }

        @Override
        public TokenStream normalize(TokenStream tokenStream) {
            return new AppendTokenFilter(tokenStream, String.valueOf(tokenFilterNumber));
        }

        @Override
        public AnalysisMode getAnalysisMode() {
            return TokenFilterFactory.super.getAnalysisMode();
        }

    }

    @NamedComponent("stableTokenizerFactory")
    public static class TestTokenizerFactory implements TokenizerFactory {
        private final java.util.List<String> tokenizerListOfChars;

        @Inject
        public TestTokenizerFactory(TestAnalysisSettings settings) {
            this.tokenizerListOfChars = settings.tokenizerListOfChars();
        }

        @Override
        public Tokenizer create() {
            return new CharSkippingTokenizer(tokenizerListOfChars);
        }

    }
}
