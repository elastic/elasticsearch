/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.fa.PersianNormalizationFilter;
import org.apache.lucene.analysis.hunspell.Dictionary;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.MappingCharFilterFactory;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.PatternReplaceCharFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.StopTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.filter1.MyFilterTokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.MatcherAssert;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AnalysisModuleTests extends ModuleTestCase {

    public IndexAnalyzers getIndexAnalyzers(Settings settings) throws IOException {
        return getIndexAnalyzers(getNewRegistry(settings), settings);
    }

    public IndexAnalyzers getIndexAnalyzers(AnalysisRegistry registry, Settings settings) throws IOException {
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        return registry.build(idxSettings);
    }

    public AnalysisRegistry getNewRegistry(Settings settings) {
        try {
            return new AnalysisModule(new Environment(settings), singletonList(new AnalysisPlugin() {
                @Override
                public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
                    return singletonMap("myfilter", MyFilterTokenFilterFactory::new);
                }
            })).getAnalysisRegistry();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Settings loadFromClasspath(String path) throws IOException {
        return Settings.builder().loadFromStream(path, getClass().getResourceAsStream(path))
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
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
    }

    public void testDefaultFactoryTokenFilters() throws IOException {
        assertTokenFilter("keyword_repeat", KeywordRepeatFilter.class);
        assertTokenFilter("persian_normalization", PersianNormalizationFilter.class);
        assertTokenFilter("arabic_normalization", ArabicNormalizationFilter.class);
    }

    public void testAnalyzerAlias() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.foobar.alias","default")
            .put("index.analysis.analyzer.foobar.type", "keyword")
            .put("index.analysis.analyzer.foobar_search.alias","default_search")
            .put("index.analysis.analyzer.foobar_search.type","english")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            // analyzer aliases are only allowed in 2.x indices
            .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_2_3_5))
            .build();
        AnalysisRegistry newRegistry = getNewRegistry(settings);
        IndexAnalyzers indexAnalyzers = getIndexAnalyzers(newRegistry, settings);
        assertThat(indexAnalyzers.get("default").analyzer(), is(instanceOf(KeywordAnalyzer.class)));
        assertThat(indexAnalyzers.get("default_search").analyzer(), is(instanceOf(EnglishAnalyzer.class)));
        assertWarnings("setting [index.analysis.analyzer.foobar.alias] is only allowed on index [test] because it was created before " +
                        "5.x; analyzer aliases can no longer be created on new indices.",
                "setting [index.analysis.analyzer.foobar_search.alias] is only allowed on index [test] because it was created before " +
                        "5.x; analyzer aliases can no longer be created on new indices.");
    }

    public void testAnalyzerAliasReferencesAlias() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.foobar.alias","default")
            .put("index.analysis.analyzer.foobar.type", "german")
            .put("index.analysis.analyzer.foobar_search.alias","default_search")
            .put("index.analysis.analyzer.foobar_search.type", "default")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            // analyzer aliases are only allowed in 2.x indices
            .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_2_3_5))
            .build();
        AnalysisRegistry newRegistry = getNewRegistry(settings);
        IndexAnalyzers indexAnalyzers = getIndexAnalyzers(newRegistry, settings);

        assertThat(indexAnalyzers.get("default").analyzer(), is(instanceOf(GermanAnalyzer.class)));
        // analyzer types are bound early before we resolve aliases
        assertThat(indexAnalyzers.get("default_search").analyzer(), is(instanceOf(StandardAnalyzer.class)));
        assertWarnings("setting [index.analysis.analyzer.foobar.alias] is only allowed on index [test] because it was created before " +
                "5.x; analyzer aliases can no longer be created on new indices.",
                "setting [index.analysis.analyzer.foobar_search.alias] is only allowed on index [test] because it was created before " +
                        "5.x; analyzer aliases can no longer be created on new indices.");
    }

    public void testAnalyzerAliasDefault() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.foobar.alias","default")
            .put("index.analysis.analyzer.foobar.type", "keyword")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            // analyzer aliases are only allowed in 2.x indices
            .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_2_3_5))
            .build();
        AnalysisRegistry newRegistry = getNewRegistry(settings);
        IndexAnalyzers indexAnalyzers = getIndexAnalyzers(newRegistry, settings);
        assertThat(indexAnalyzers.get("default").analyzer(), is(instanceOf(KeywordAnalyzer.class)));
        assertThat(indexAnalyzers.get("default_search").analyzer(), is(instanceOf(KeywordAnalyzer.class)));
        assertWarnings("setting [index.analysis.analyzer.foobar.alias] is only allowed on index [test] because it was created before " +
                "5.x; analyzer aliases can no longer be created on new indices.");
    }

    public void testAnalyzerAliasMoreThanOnce() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.foobar.alias","default")
            .put("index.analysis.analyzer.foobar.type", "keyword")
            .put("index.analysis.analyzer.foobar1.alias","default")
            .put("index.analysis.analyzer.foobar1.type", "english")
            // analyzer aliases are only allowed in 2.x indices
            .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_2_3_5))
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        AnalysisRegistry newRegistry = getNewRegistry(settings);
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> getIndexAnalyzers(newRegistry, settings));
        assertEquals("alias [default] is already used by [foobar]", ise.getMessage());
        assertWarnings("setting [index.analysis.analyzer.foobar.alias] is only allowed on index [test] because it was created before " +
                "5.x; analyzer aliases can no longer be created on new indices.",
                "setting [index.analysis.analyzer.foobar1.alias] is only allowed on index [test] because it was created before " +
                        "5.x; analyzer aliases can no longer be created on new indices.");
    }

    public void testAnalyzerAliasNotAllowedPost5x() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.foobar.type", "standard")
            .put("index.analysis.analyzer.foobar.alias","foobaz")
            // analyzer aliases were removed in v5.0.0 alpha6
            .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_5_0_0_beta1, null))
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        AnalysisRegistry registry = getNewRegistry(settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> getIndexAnalyzers(registry, settings));
        assertEquals("setting [index.analysis.analyzer.foobar.alias] is not supported", e.getMessage());
    }

    public void testVersionedAnalyzers() throws Exception {
        String yaml = "/org/elasticsearch/index/analysis/test1.yml";
        Settings settings2 = Settings.builder()
                .loadFromStream(yaml, getClass().getResourceAsStream(yaml))
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_0_0)
                .build();
        AnalysisRegistry newRegistry = getNewRegistry(settings2);
        IndexAnalyzers indexAnalyzers = getIndexAnalyzers(newRegistry, settings2);

        // registry always has the current version
        assertThat(newRegistry.getAnalyzer("default"), is(instanceOf(NamedAnalyzer.class)));
        NamedAnalyzer defaultNamedAnalyzer = (NamedAnalyzer) newRegistry.getAnalyzer("default");
        assertThat(defaultNamedAnalyzer.analyzer(), is(instanceOf(StandardAnalyzer.class)));
        assertEquals(Version.CURRENT.luceneVersion, defaultNamedAnalyzer.analyzer().getVersion());

        // analysis service has the expected version
        assertThat(indexAnalyzers.get("standard").analyzer(), is(instanceOf(StandardAnalyzer.class)));
        assertEquals(Version.V_2_0_0.luceneVersion, indexAnalyzers.get("standard").analyzer().getVersion());
        assertEquals(Version.V_2_0_0.luceneVersion, indexAnalyzers.get("thai").analyzer().getVersion());

        assertThat(indexAnalyzers.get("custom7").analyzer(), is(instanceOf(StandardAnalyzer.class)));
        assertEquals(org.apache.lucene.util.Version.fromBits(3,6,0), indexAnalyzers.get("custom7").analyzer().getVersion());
    }

    private void assertTokenFilter(String name, Class<?> clazz) throws IOException {
        Settings settings = Settings.builder()
                               .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                               .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings);
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get(name);
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("foo bar"));
        TokenStream stream = tokenFilter.create(tokenizer);
        assertThat(stream, instanceOf(clazz));
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

        analyzer = indexAnalyzers.get("custom2").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));

        // verify position increment gap
        analyzer = indexAnalyzers.get("custom6").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom6 = (CustomAnalyzer) analyzer;
        assertThat(custom6.getPositionIncrementGap("any_string"), equalTo(256));

        // verify characters  mapping
        analyzer = indexAnalyzers.get("custom5").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom5 = (CustomAnalyzer) analyzer;
        assertThat(custom5.charFilters()[0], instanceOf(MappingCharFilterFactory.class));

        // check custom pattern replace filter
        analyzer = indexAnalyzers.get("custom3").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom3 = (CustomAnalyzer) analyzer;
        PatternReplaceCharFilterFactory patternReplaceCharFilterFactory = (PatternReplaceCharFilterFactory) custom3.charFilters()[0];
        assertThat(patternReplaceCharFilterFactory.getPattern().pattern(), equalTo("sample(.*)"));
        assertThat(patternReplaceCharFilterFactory.getReplacement(), equalTo("replacedSample $1"));

        // check custom class name (my)
        analyzer = indexAnalyzers.get("custom4").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom4 = (CustomAnalyzer) analyzer;
        assertThat(custom4.tokenFilters()[0], instanceOf(MyFilterTokenFilterFactory.class));

//        // verify Czech stemmer
//        analyzer = analysisService.analyzer("czechAnalyzerWithStemmer").analyzer();
//        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
//        CustomAnalyzer czechstemmeranalyzer = (CustomAnalyzer) analyzer;
//        assertThat(czechstemmeranalyzer.tokenizerFactory(), instanceOf(StandardTokenizerFactory.class));
//        assertThat(czechstemmeranalyzer.tokenFilters().length, equalTo(4));
//        assertThat(czechstemmeranalyzer.tokenFilters()[3], instanceOf(CzechStemTokenFilterFactory.class));
//
//        // check dictionary decompounder
//        analyzer = analysisService.analyzer("decompoundingAnalyzer").analyzer();
//        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
//        CustomAnalyzer dictionaryDecompounderAnalyze = (CustomAnalyzer) analyzer;
//        assertThat(dictionaryDecompounderAnalyze.tokenizerFactory(), instanceOf(StandardTokenizerFactory.class));
//        assertThat(dictionaryDecompounderAnalyze.tokenFilters().length, equalTo(1));
//        assertThat(dictionaryDecompounderAnalyze.tokenFilters()[0], instanceOf(DictionaryCompoundWordTokenFilterFactory.class));

        Set<?> wordList = Analysis.getWordSet(null, Version.CURRENT, settings, "index.analysis.filter.dict_dec.word_list");
        MatcherAssert.assertThat(wordList.size(), equalTo(6));
//        MatcherAssert.assertThat(wordList, hasItems("donau", "dampf", "schiff", "spargel", "creme", "suppe"));
    }

    public void testWordListPath() throws Exception {
        Settings settings = Settings.builder()
                               .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                               .build();
        Environment env = new Environment(settings);
        String[] words = new String[]{"donau", "dampf", "schiff", "spargel", "creme", "suppe"};

        Path wordListFile = generateWordList(words);
        settings = Settings.builder().loadFromSource("index: \n  word_list_path: " + wordListFile.toAbsolutePath(), XContentType.YAML)
            .build();

        Set<?> wordList = Analysis.getWordSet(env, Version.CURRENT, settings, "index.word_list");
        MatcherAssert.assertThat(wordList.size(), equalTo(6));
//        MatcherAssert.assertThat(wordList, hasItems(words));
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
                .put("index.analysis.analyzer._invalid_name.tokenizer", "keyword")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, "1")
                .build();
        try {
            getIndexAnalyzers(settings);
            fail("This should fail with IllegalArgumentException because the analyzers name starts with _");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), either(equalTo("analyzer name must not start with '_'. got \"_invalid_name\""))
                    .or(equalTo("analyzer name must not start with '_'. got \"_invalidName\"")));
        }
    }

    public void testUnderscoreInAnalyzerNameAlias() throws IOException {
        Settings settings = Settings.builder()
                .put("index.analysis.analyzer.valid_name.tokenizer", "keyword")
                .put("index.analysis.analyzer.valid_name.alias", "_invalid_name")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                // analyzer aliases are only allowed for 2.x indices
                .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_2_3_5))
                .build();
        try {
            getIndexAnalyzers(settings);
            fail("This should fail with IllegalArgumentException because the analyzers alias starts with _");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("analyzer name must not start with '_'. got \"_invalid_name\""));
        }
        assertWarnings("setting [index.analysis.analyzer.valid_name.alias] is only allowed on index [test] because it was " +
                "created before 5.x; analyzer aliases can no longer be created on new indices.");
    }

    public void testDeprecatedPositionOffsetGap() throws IOException {
        Settings settings = Settings.builder()
                .put("index.analysis.analyzer.custom.tokenizer", "standard")
                .put("index.analysis.analyzer.custom.position_offset_gap", "128")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        try {
            getIndexAnalyzers(settings);
            fail("Analyzer should fail if it has position_offset_gap");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Option [position_offset_gap] in Custom Analyzer [custom] " +
                    "has been renamed, please use [position_increment_gap] instead."));
        }
    }

    public void testRegisterHunspellDictionary() throws Exception {
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        Environment environment = new Environment(settings);
        InputStream aff = getClass().getResourceAsStream("/indices/analyze/conf_dir/hunspell/en_US/en_US.aff");
        InputStream dic = getClass().getResourceAsStream("/indices/analyze/conf_dir/hunspell/en_US/en_US.dic");
        Dictionary dictionary;
        try (Directory tmp = new SimpleFSDirectory(environment.tmpFile())) {
            dictionary = new Dictionary(tmp, "hunspell", aff, dic);
        }
        AnalysisModule module = new AnalysisModule(environment, singletonList(new AnalysisPlugin() {
            @Override
            public Map<String, Dictionary> getHunspellDictionaries() {
                return singletonMap("foo", dictionary);
            }
        }));
        assertSame(dictionary, module.getHunspellService().getDictionary("foo"));
    }
}
