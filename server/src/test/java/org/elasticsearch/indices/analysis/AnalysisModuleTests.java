/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.hunspell.Dictionary;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.MyFilterTokenFilterFactory;
import org.elasticsearch.index.analysis.PreConfiguredCharFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.StopTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.indices.analysis.lucene.AppendCharFilter;
import org.elasticsearch.indices.analysis.lucene.AppendTokenFilter;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.MatcherAssert;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AnalysisModuleTests extends ESTestCase {
    private final Settings emptyNodeSettings = Settings.builder()
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();

    public IndexAnalyzers getIndexAnalyzers(Settings settings) throws IOException {
        return getIndexAnalyzers(getNewRegistry(settings), settings);
    }

    public IndexAnalyzers getIndexAnalyzers(AnalysisRegistry registry, Settings settings) throws IOException {
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        return registry.build(IndexCreationContext.CREATE_INDEX, idxSettings);
    }

    public AnalysisRegistry getNewRegistry(Settings settings) {
        try {
            return new AnalysisModule(TestEnvironment.newEnvironment(settings), singletonList(new AnalysisPlugin() {
                @Override
                public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
                    return singletonMap("myfilter", MyFilterTokenFilterFactory::new);
                }

                @Override
                public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
                    return AnalysisPlugin.super.getCharFilters();
                }
            }), new StablePluginsRegistry()).getAnalysisRegistry();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Settings loadFromClasspath(String path) throws IOException {
        return Settings.builder()
            .loadFromStream(path, getClass().getResourceAsStream(path), false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();

    }

    public void testSimpleConfigurationJson() throws IOException {
        Settings settings = loadFromClasspath("/org/elasticsearch/index/analysis/test1.json");
        testSimpleConfiguration(settings);
    }

    public void testSimpleConfigurationYaml() throws IOException {
        Settings settings = loadFromClasspath("/org/elasticsearch/index/analysis/test1.yml");
        testSimpleConfiguration(settings);
        assertWarnings("Setting [version] on analysis component [custom7] has no effect and is deprecated");
    }

    private void testSimpleConfiguration(Settings settings) throws IOException {
        IndexAnalyzers indexAnalyzers = getIndexAnalyzers(settings);
        Analyzer analyzer = indexAnalyzers.get("custom1").analyzer();

        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom1 = (CustomAnalyzer) analyzer;
        assertThat(custom1.tokenizerFactory(), instanceOf(StandardTokenizerFactory.class));
        assertThat(custom1.tokenFilters().length, equalTo(2));

        StopTokenFilterFactory stop1 = (StopTokenFilterFactory) custom1.tokenFilters()[0];
        assertThat(stop1.stopWords().size(), equalTo(1));

        // verify position increment gap
        analyzer = indexAnalyzers.get("custom6").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom6 = (CustomAnalyzer) analyzer;
        assertThat(custom6.getPositionIncrementGap("any_string"), equalTo(256));

        // check custom class name (my)
        analyzer = indexAnalyzers.get("custom4").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom4 = (CustomAnalyzer) analyzer;
        assertThat(custom4.tokenFilters()[0], instanceOf(MyFilterTokenFilterFactory.class));
    }

    public void testWordListPath() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        String[] words = new String[] { "donau", "dampf", "schiff", "spargel", "creme", "suppe" };

        Path wordListFile = generateWordList(words);
        settings = Settings.builder()
            .loadFromSource("index: \n  word_list_path: " + wordListFile.toAbsolutePath(), XContentType.YAML)
            .build();

        Set<?> wordList = Analysis.getWordSet(env, settings, "index.word_list");
        MatcherAssert.assertThat(wordList.size(), equalTo(6));
        // MatcherAssert.assertThat(wordList, hasItems(words));
        Files.delete(wordListFile);
    }

    private Path generateWordList(String[] words) throws Exception {
        Path wordListFile = createTempDir().resolve("wordlist.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(wordListFile, StandardCharsets.UTF_8)) {
            for (String word : words) {
                writer.write(word);
                writer.write('\n');
            }
        }
        return wordListFile;
    }

    public void testUnderscoreInAnalyzerName() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer._invalid_name.tokenizer", "standard")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, "1")
            .build();
        try {
            getIndexAnalyzers(settings);
            fail("This should fail with IllegalArgumentException because the analyzers name starts with _");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                either(equalTo("analyzer name must not start with '_'. got \"_invalid_name\"")).or(
                    equalTo("analyzer name must not start with '_'. got \"_invalidName\"")
                )
            );
        }
    }

    public void testStandardFilterBWC() throws IOException {
        // standard tokenfilter should have been removed entirely in the 7x line. However, a
        // cacheing bug meant that it was still possible to create indexes using a standard
        // filter until 7.6
        {
            Version version = VersionUtils.randomVersionBetween(random(), Version.V_7_6_0, Version.CURRENT);
            final Settings settings = Settings.builder()
                .put("index.analysis.analyzer.my_standard.tokenizer", "standard")
                .put("index.analysis.analyzer.my_standard.filter", "standard")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                .build();
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> getIndexAnalyzers(settings));
            assertThat(exc.getMessage(), equalTo("The [standard] token filter has been removed."));
        }
        {
            Version version = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_5_2);
            final Settings settings = Settings.builder()
                .put("index.analysis.analyzer.my_standard.tokenizer", "standard")
                .put("index.analysis.analyzer.my_standard.filter", "standard")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                .build();
            getIndexAnalyzers(settings);
            assertWarnings("The [standard] token filter is deprecated and will be removed in a future version.");
        }
    }

    /**
     * Tests that plugins can register pre-configured char filters that vary in behavior based on Elasticsearch version, Lucene version,
     * and that do not vary based on version at all.
     */
    public void testPluginPreConfiguredCharFilters() throws IOException {
        boolean noVersionSupportsMultiTerm = randomBoolean();
        boolean luceneVersionSupportsMultiTerm = randomBoolean();
        boolean indexVersionSupportsMultiTerm = randomBoolean();
        AnalysisRegistry registry = new AnalysisModule(
            TestEnvironment.newEnvironment(emptyNodeSettings),
            singletonList(new AnalysisPlugin() {
                @Override
                public List<PreConfiguredCharFilter> getPreConfiguredCharFilters() {
                    return Arrays.asList(
                        PreConfiguredCharFilter.singleton(
                            "no_version",
                            noVersionSupportsMultiTerm,
                            tokenStream -> new AppendCharFilter(tokenStream, "no_version")
                        ),
                        PreConfiguredCharFilter.luceneVersion(
                            "lucene_version",
                            luceneVersionSupportsMultiTerm,
                            (tokenStream, luceneVersion) -> new AppendCharFilter(tokenStream, luceneVersion.toString())
                        ),
                        PreConfiguredCharFilter.indexVersion(
                            "index_version",
                            indexVersionSupportsMultiTerm,
                            (tokenStream, esVersion) -> new AppendCharFilter(tokenStream, esVersion.toString())
                        )
                    );
                }

                @Override
                public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
                    // Need mock keyword tokenizer here, because alpha / beta versions are broken up by the dash.
                    return singletonMap(
                        "keyword",
                        (indexSettings, environment, name, settings) -> TokenizerFactory.newFactory(
                            name,
                            () -> new MockTokenizer(MockTokenizer.KEYWORD, false)
                        )
                    );
                }
            }),
            new StablePluginsRegistry()
        ).getAnalysisRegistry();

        IndexVersion version = IndexVersionUtils.randomVersion(random());
        IndexAnalyzers analyzers = getIndexAnalyzers(
            registry,
            Settings.builder()
                .put("index.analysis.analyzer.no_version.tokenizer", "keyword")
                .put("index.analysis.analyzer.no_version.char_filter", "no_version")
                .put("index.analysis.analyzer.lucene_version.tokenizer", "keyword")
                .put("index.analysis.analyzer.lucene_version.char_filter", "lucene_version")
                .put("index.analysis.analyzer.index_version.tokenizer", "keyword")
                .put("index.analysis.analyzer.index_version.char_filter", "index_version")
                .put(IndexMetadata.SETTING_VERSION_CREATED, version.id())
                .build()
        );
        assertTokenStreamContents(analyzers.get("no_version").tokenStream("", "test"), new String[] { "testno_version" });
        assertTokenStreamContents(
            analyzers.get("lucene_version").tokenStream("", "test"),
            new String[] { "test" + version.luceneVersion() }
        );
        assertTokenStreamContents(analyzers.get("index_version").tokenStream("", "test"), new String[] { "test" + version });

        assertEquals(
            "test" + (noVersionSupportsMultiTerm ? "no_version" : ""),
            analyzers.get("no_version").normalize("", "test").utf8ToString()
        );
        assertEquals(
            "test" + (luceneVersionSupportsMultiTerm ? version.luceneVersion().toString() : ""),
            analyzers.get("lucene_version").normalize("", "test").utf8ToString()
        );
        assertEquals(
            "test" + (indexVersionSupportsMultiTerm ? version.toString() : ""),
            analyzers.get("index_version").normalize("", "test").utf8ToString()
        );
    }

    /**
     * Tests that plugins can register pre-configured token filters that vary in behavior based on Elasticsearch version, Lucene version,
     * and that do not vary based on version at all.
     */
    public void testPluginPreConfiguredTokenFilters() throws IOException {
        boolean noVersionSupportsMultiTerm = randomBoolean();
        boolean luceneVersionSupportsMultiTerm = randomBoolean();
        boolean indexVersionSupportsMultiTerm = randomBoolean();
        AnalysisRegistry registry = new AnalysisModule(
            TestEnvironment.newEnvironment(emptyNodeSettings),
            singletonList(new AnalysisPlugin() {
                @Override
                public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
                    return Arrays.asList(
                        PreConfiguredTokenFilter.singleton(
                            "no_version",
                            noVersionSupportsMultiTerm,
                            tokenStream -> new AppendTokenFilter(tokenStream, "no_version")
                        ),
                        PreConfiguredTokenFilter.luceneVersion(
                            "lucene_version",
                            luceneVersionSupportsMultiTerm,
                            (tokenStream, luceneVersion) -> new AppendTokenFilter(tokenStream, luceneVersion.toString())
                        ),
                        PreConfiguredTokenFilter.indexVersion(
                            "index_version",
                            indexVersionSupportsMultiTerm,
                            (tokenStream, esVersion) -> new AppendTokenFilter(tokenStream, esVersion.toString())
                        )
                    );
                }
            }),
            new StablePluginsRegistry()
        ).getAnalysisRegistry();

        IndexVersion version = IndexVersionUtils.randomVersion(random());
        IndexAnalyzers analyzers = getIndexAnalyzers(
            registry,
            Settings.builder()
                .put("index.analysis.analyzer.no_version.tokenizer", "standard")
                .put("index.analysis.analyzer.no_version.filter", "no_version")
                .put("index.analysis.analyzer.lucene_version.tokenizer", "standard")
                .put("index.analysis.analyzer.lucene_version.filter", "lucene_version")
                .put("index.analysis.analyzer.index_version.tokenizer", "standard")
                .put("index.analysis.analyzer.index_version.filter", "index_version")
                .put(IndexMetadata.SETTING_VERSION_CREATED, version.id())
                .build()
        );
        assertTokenStreamContents(analyzers.get("no_version").tokenStream("", "test"), new String[] { "testno_version" });
        assertTokenStreamContents(
            analyzers.get("lucene_version").tokenStream("", "test"),
            new String[] { "test" + version.luceneVersion() }
        );
        assertTokenStreamContents(analyzers.get("index_version").tokenStream("", "test"), new String[] { "test" + version });

        assertEquals(
            "test" + (noVersionSupportsMultiTerm ? "no_version" : ""),
            analyzers.get("no_version").normalize("", "test").utf8ToString()
        );
        assertEquals(
            "test" + (luceneVersionSupportsMultiTerm ? version.luceneVersion().toString() : ""),
            analyzers.get("lucene_version").normalize("", "test").utf8ToString()
        );
        assertEquals(
            "test" + (indexVersionSupportsMultiTerm ? version.toString() : ""),
            analyzers.get("index_version").normalize("", "test").utf8ToString()
        );
    }

    /**
     * Tests that plugins can register pre-configured token filters that vary in behavior based on Elasticsearch version, Lucene version,
     * and that do not vary based on version at all.
     */
    public void testPluginPreConfiguredTokenizers() throws IOException {

        // Simple tokenizer that always spits out a single token with some preconfigured characters
        final class FixedTokenizer extends Tokenizer {
            private final CharTermAttribute term = addAttribute(CharTermAttribute.class);
            private final char[] chars;
            private boolean read = false;

            protected FixedTokenizer(String chars) {
                this.chars = chars.toCharArray();
            }

            @Override
            public boolean incrementToken() throws IOException {
                if (read) {
                    return false;
                }
                clearAttributes();
                read = true;
                term.resizeBuffer(chars.length);
                System.arraycopy(chars, 0, term.buffer(), 0, chars.length);
                term.setLength(chars.length);
                return true;
            }

            @Override
            public void reset() throws IOException {
                super.reset();
                read = false;
            }
        }
        AnalysisRegistry registry = new AnalysisModule(
            TestEnvironment.newEnvironment(emptyNodeSettings),
            singletonList(new AnalysisPlugin() {
                @Override
                public List<PreConfiguredTokenizer> getPreConfiguredTokenizers() {
                    return Arrays.asList(
                        PreConfiguredTokenizer.singleton("no_version", () -> new FixedTokenizer("no_version")),
                        PreConfiguredTokenizer.luceneVersion(
                            "lucene_version",
                            luceneVersion -> new FixedTokenizer(luceneVersion.toString())
                        ),
                        PreConfiguredTokenizer.indexVersion("index_version", indexVersion -> new FixedTokenizer(indexVersion.toString()))
                    );
                }
            }),
            new StablePluginsRegistry()
        ).getAnalysisRegistry();

        IndexVersion version = IndexVersionUtils.randomVersion(random());
        IndexAnalyzers analyzers = getIndexAnalyzers(
            registry,
            Settings.builder()
                .put("index.analysis.analyzer.no_version.tokenizer", "no_version")
                .put("index.analysis.analyzer.lucene_version.tokenizer", "lucene_version")
                .put("index.analysis.analyzer.index_version.tokenizer", "index_version")
                .put(IndexMetadata.SETTING_VERSION_CREATED, version.id())
                .build()
        );
        assertTokenStreamContents(analyzers.get("no_version").tokenStream("", "test"), new String[] { "no_version" });
        assertTokenStreamContents(
            analyzers.get("lucene_version").tokenStream("", "test"),
            new String[] { version.luceneVersion().toString() }
        );
        assertTokenStreamContents(analyzers.get("index_version").tokenStream("", "test"), new String[] { version.toString() });

        // These are current broken by https://github.com/elastic/elasticsearch/issues/24752
        // assertEquals("test" + (noVersionSupportsMultiTerm ? "no_version" : ""),
        // analyzers.get("no_version").normalize("", "test").utf8ToString());
        // assertEquals("test" + (luceneVersionSupportsMultiTerm ? version.luceneVersion.toString() : ""),
        // analyzers.get("lucene_version").normalize("", "test").utf8ToString());
        // assertEquals("test" + (elasticsearchVersionSupportsMultiTerm ? version.toString() : ""),
        // analyzers.get("index_version").normalize("", "test").utf8ToString());
    }

    public void testRegisterHunspellDictionary() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        Environment environment = TestEnvironment.newEnvironment(settings);
        InputStream aff = getClass().getResourceAsStream("/indices/analyze/conf_dir/hunspell/en_US/en_US.aff");
        InputStream dic = getClass().getResourceAsStream("/indices/analyze/conf_dir/hunspell/en_US/en_US.dic");
        Dictionary dictionary;
        try (Directory tmp = newFSDirectory(environment.tmpFile())) {
            dictionary = new Dictionary(tmp, "hunspell", aff, dic);
        }
        AnalysisModule module = new AnalysisModule(environment, singletonList(new AnalysisPlugin() {
            @Override
            public Map<String, Dictionary> getHunspellDictionaries() {
                return singletonMap("foo", dictionary);
            }
        }), new StablePluginsRegistry());
        assertSame(dictionary, module.getHunspellService().getDictionary("foo"));
    }

}
