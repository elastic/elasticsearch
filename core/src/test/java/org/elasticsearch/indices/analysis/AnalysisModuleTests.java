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
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.CustomAnalyzer;
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

/**
 *
 */
public class AnalysisModuleTests extends ModuleTestCase {

    public AnalysisService getAnalysisService(Settings settings) throws IOException {
        return getAnalysisService(getNewRegistry(settings), settings);
    }

    public AnalysisService getAnalysisService(AnalysisRegistry registry, Settings settings) throws IOException {
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
            .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
            .build();
        AnalysisRegistry newRegistry = getNewRegistry(settings);
        AnalysisService as = getAnalysisService(newRegistry, settings);
        assertThat(as.analyzer("default").analyzer(), is(instanceOf(KeywordAnalyzer.class)));
        assertThat(as.analyzer("default_search").analyzer(), is(instanceOf(EnglishAnalyzer.class)));
    }

    public void testAnalyzerAliasReferencesAlias() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.foobar.alias","default")
            .put("index.analysis.analyzer.foobar.type", "german")
            .put("index.analysis.analyzer.foobar_search.alias","default_search")
            .put("index.analysis.analyzer.foobar_search.type", "default")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
            .build();
        AnalysisRegistry newRegistry = getNewRegistry(settings);
        AnalysisService as = getAnalysisService(newRegistry, settings);
        assertThat(as.analyzer("default").analyzer(), is(instanceOf(GermanAnalyzer.class)));
        // analyzer types are bound early before we resolve aliases
        assertThat(as.analyzer("default_search").analyzer(), is(instanceOf(StandardAnalyzer.class)));
    }

    public void testAnalyzerAliasDefault() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.foobar.alias","default")
            .put("index.analysis.analyzer.foobar.type", "keyword")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
            .build();
        AnalysisRegistry newRegistry = getNewRegistry(settings);
        AnalysisService as = getAnalysisService(newRegistry, settings);
        assertThat(as.analyzer("default").analyzer(), is(instanceOf(KeywordAnalyzer.class)));
        assertThat(as.analyzer("default_search").analyzer(), is(instanceOf(KeywordAnalyzer.class)));
    }

    public void testAnalyzerAliasMoreThanOnce() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.foobar.alias","default")
            .put("index.analysis.analyzer.foobar.type", "keyword")
            .put("index.analysis.analyzer.foobar1.alias","default")
            .put("index.analysis.analyzer.foobar1.type", "english")
            .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        AnalysisRegistry newRegistry = getNewRegistry(settings);
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> getAnalysisService(newRegistry, settings));
        assertEquals("alias [default] is already used by [foobar]", ise.getMessage());
    }
    public void testVersionedAnalyzers() throws Exception {
        String yaml = "/org/elasticsearch/index/analysis/test1.yml";
        Settings settings2 = Settings.builder()
                .loadFromStream(yaml, getClass().getResourceAsStream(yaml))
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_0_0)
                .build();
        AnalysisRegistry newRegistry = getNewRegistry(settings2);
        AnalysisService analysisService2 = getAnalysisService(newRegistry, settings2);

        // registry always has the current version
        assertThat(newRegistry.getAnalyzer("default"), is(instanceOf(NamedAnalyzer.class)));
        NamedAnalyzer defaultNamedAnalyzer = (NamedAnalyzer) newRegistry.getAnalyzer("default");
        assertThat(defaultNamedAnalyzer.analyzer(), is(instanceOf(StandardAnalyzer.class)));
        assertEquals(Version.CURRENT.luceneVersion, defaultNamedAnalyzer.analyzer().getVersion());

        // analysis service has the expected version
        assertThat(analysisService2.analyzer("standard").analyzer(), is(instanceOf(StandardAnalyzer.class)));
        assertEquals(Version.V_2_0_0.luceneVersion, analysisService2.analyzer("standard").analyzer().getVersion());
        assertEquals(Version.V_2_0_0.luceneVersion, analysisService2.analyzer("thai").analyzer().getVersion());

        assertThat(analysisService2.analyzer("custom7").analyzer(), is(instanceOf(StandardAnalyzer.class)));
        assertEquals(org.apache.lucene.util.Version.fromBits(3,6,0), analysisService2.analyzer("custom7").analyzer().getVersion());
    }

    private void assertTokenFilter(String name, Class<?> clazz) throws IOException {
        Settings settings = Settings.builder()
                               .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                               .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter(name);
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("foo bar"));
        TokenStream stream = tokenFilter.create(tokenizer);
        assertThat(stream, instanceOf(clazz));
    }

    private void testSimpleConfiguration(Settings settings) throws IOException {
        AnalysisService analysisService = getAnalysisService(settings);
        Analyzer analyzer = analysisService.analyzer("custom1").analyzer();

        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom1 = (CustomAnalyzer) analyzer;
        assertThat(custom1.tokenizerFactory(), instanceOf(StandardTokenizerFactory.class));
        assertThat(custom1.tokenFilters().length, equalTo(2));

        StopTokenFilterFactory stop1 = (StopTokenFilterFactory) custom1.tokenFilters()[0];
        assertThat(stop1.stopWords().size(), equalTo(1));

        analyzer = analysisService.analyzer("custom2").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));

        // verify position increment gap
        analyzer = analysisService.analyzer("custom6").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom6 = (CustomAnalyzer) analyzer;
        assertThat(custom6.getPositionIncrementGap("any_string"), equalTo(256));

        // verify characters  mapping
        analyzer = analysisService.analyzer("custom5").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom5 = (CustomAnalyzer) analyzer;
        assertThat(custom5.charFilters()[0], instanceOf(MappingCharFilterFactory.class));

        // verify aliases
        analyzer = analysisService.analyzer("alias1").analyzer();
        assertThat(analyzer, instanceOf(StandardAnalyzer.class));

        // check custom pattern replace filter
        analyzer = analysisService.analyzer("custom3").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom3 = (CustomAnalyzer) analyzer;
        PatternReplaceCharFilterFactory patternReplaceCharFilterFactory = (PatternReplaceCharFilterFactory) custom3.charFilters()[0];
        assertThat(patternReplaceCharFilterFactory.getPattern().pattern(), equalTo("sample(.*)"));
        assertThat(patternReplaceCharFilterFactory.getReplacement(), equalTo("replacedSample $1"));

        // check custom class name (my)
        analyzer = analysisService.analyzer("custom4").analyzer();
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

        Set<?> wordList = Analysis.getWordSet(null, settings, "index.analysis.filter.dict_dec.word_list");
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
        settings = Settings.builder().loadFromSource("index: \n  word_list_path: " + wordListFile.toAbsolutePath()).build();

        Set<?> wordList = Analysis.getWordSet(env, settings, "index.word_list");
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
            getAnalysisService(settings);
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
                .put(IndexMetaData.SETTING_VERSION_CREATED, "1")
                .build();
        try {
            getAnalysisService(settings);
            fail("This should fail with IllegalArgumentException because the analyzers alias starts with _");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("analyzer name must not start with '_'. got \"_invalid_name\""));
        }
    }

    public void testDeprecatedPositionOffsetGap() throws IOException {
        Settings settings = Settings.builder()
                .put("index.analysis.analyzer.custom.tokenizer", "standard")
                .put("index.analysis.analyzer.custom.position_offset_gap", "128")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        try {
            getAnalysisService(settings);
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
