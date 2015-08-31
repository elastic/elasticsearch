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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.fa.PersianNormalizationFilter;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.ProvisionException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.filter1.MyFilterTokenFilterFactory;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.MatcherAssert;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class AnalysisModuleTests extends ESTestCase {

    private Injector injector;

    public AnalysisService getAnalysisService(Settings settings) {
        Index index = new Index("test");
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(settings), new EnvironmentModule(new Environment(settings))).createInjector();
        AnalysisModule analysisModule = new AnalysisModule(settings, parentInjector.getInstance(IndicesAnalysisService.class));
        analysisModule.addTokenFilter("myfilter", MyFilterTokenFilterFactory.class);
        injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                analysisModule)
            .createChildInjector(parentInjector);

        return injector.getInstance(AnalysisService.class);
    }

    private Settings loadFromClasspath(String path) {
        return settingsBuilder().loadFromStream(path, getClass().getResourceAsStream(path))
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("path.home", createTempDir().toString())
                .build();

    }

    public void testSimpleConfigurationJson() {
        Settings settings = loadFromClasspath("/org/elasticsearch/index/analysis/test1.json");
        testSimpleConfiguration(settings);
    }

    public void testSimpleConfigurationYaml() {
        Settings settings = loadFromClasspath("/org/elasticsearch/index/analysis/test1.yml");
        testSimpleConfiguration(settings);
    }

    public void testDefaultFactoryTokenFilters() throws IOException {
        assertTokenFilter("keyword_repeat", KeywordRepeatFilter.class);
        assertTokenFilter("persian_normalization", PersianNormalizationFilter.class);
        assertTokenFilter("arabic_normalization", ArabicNormalizationFilter.class);
    }

    public void testVersionedAnalyzers() throws Exception {
        String yaml = "/org/elasticsearch/index/analysis/test1.yml";
        Settings settings2 = settingsBuilder()
                .loadFromStream(yaml, getClass().getResourceAsStream(yaml))
                .put("path.home", createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_0_90_0)
                .build();
        AnalysisService analysisService2 = getAnalysisService(settings2);

        // indicesanalysisservice always has the current version
        IndicesAnalysisService indicesAnalysisService2 = injector.getInstance(IndicesAnalysisService.class);
        assertThat(indicesAnalysisService2.analyzer("default"), is(instanceOf(NamedAnalyzer.class)));
        NamedAnalyzer defaultNamedAnalyzer = (NamedAnalyzer) indicesAnalysisService2.analyzer("default");
        assertThat(defaultNamedAnalyzer.analyzer(), is(instanceOf(StandardAnalyzer.class)));
        assertEquals(Version.CURRENT.luceneVersion, defaultNamedAnalyzer.analyzer().getVersion());

        // analysis service has the expected version
        assertThat(analysisService2.analyzer("standard").analyzer(), is(instanceOf(StandardAnalyzer.class)));
        assertEquals(Version.V_0_90_0.luceneVersion, analysisService2.analyzer("standard").analyzer().getVersion());
        assertEquals(Version.V_0_90_0.luceneVersion, analysisService2.analyzer("thai").analyzer().getVersion());
    }

    private void assertTokenFilter(String name, Class clazz) throws IOException {
        Settings settings = Settings.settingsBuilder()
                               .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                               .put("path.home", createTempDir().toString()).build();
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter(name);
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("foo bar"));
        TokenStream stream = tokenFilter.create(tokenizer);
        assertThat(stream, instanceOf(clazz));
    }

    private void testSimpleConfiguration(Settings settings) {
        AnalysisService analysisService = getAnalysisService(settings);
        Analyzer analyzer = analysisService.analyzer("custom1").analyzer();

        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom1 = (CustomAnalyzer) analyzer;
        assertThat(custom1.tokenizerFactory(), instanceOf(StandardTokenizerFactory.class));
        assertThat(custom1.tokenFilters().length, equalTo(2));

        StopTokenFilterFactory stop1 = (StopTokenFilterFactory) custom1.tokenFilters()[0];
        assertThat(stop1.stopWords().size(), equalTo(1));
        //assertThat((Iterable<char[]>) stop1.stopWords(), hasItem("test-stop".toCharArray()));

        analyzer = analysisService.analyzer("custom2").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom2 = (CustomAnalyzer) analyzer;

//        HtmlStripCharFilterFactory html = (HtmlStripCharFilterFactory) custom2.charFilters()[0];
//        assertThat(html.readAheadLimit(), equalTo(HTMLStripCharFilter.DEFAULT_READ_AHEAD));
//
//        html = (HtmlStripCharFilterFactory) custom2.charFilters()[1];
//        assertThat(html.readAheadLimit(), equalTo(1024));

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
                               .put("path.home", createTempDir().toString())
                               .build();
        Environment env = new Environment(settings);
        String[] words = new String[]{"donau", "dampf", "schiff", "spargel", "creme", "suppe"};

        Path wordListFile = generateWordList(words);
        settings = settingsBuilder().loadFromSource("index: \n  word_list_path: " + wordListFile.toAbsolutePath()).build();

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

    public void testUnderscoreInAnalyzerName() {
        Settings settings = Settings.builder()
                .put("index.analysis.analyzer._invalid_name.tokenizer", "keyword")
                .put("path.home", createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, "1")
                .build();
        try {
            getAnalysisService(settings);
            fail("This should fail with IllegalArgumentException because the analyzers name starts with _");
        } catch (ProvisionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertThat(e.getCause().getMessage(), either(equalTo("analyzer name must not start with '_'. got \"_invalid_name\"")).or(equalTo("analyzer name must not start with '_'. got \"_invalidName\"")));
        }
    }

    public void testUnderscoreInAnalyzerNameAlias() {
        Settings settings = Settings.builder()
                .put("index.analysis.analyzer.valid_name.tokenizer", "keyword")
                .put("index.analysis.analyzer.valid_name.alias", "_invalid_name")
                .put("path.home", createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, "1")
                .build();
        try {
            getAnalysisService(settings);
            fail("This should fail with IllegalArgumentException because the analyzers alias starts with _");
        } catch (ProvisionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertThat(e.getCause().getMessage(), equalTo("analyzer name must not start with '_'. got \"_invalid_name\""));
        }
    }

    public void testBackwardCompatible() {
        Settings settings = settingsBuilder()
                .put("index.analysis.analyzer.custom1.tokenizer", "standard")
                .put("index.analysis.analyzer.custom1.position_offset_gap", "128")
                .put("index.analysis.analyzer.custom2.tokenizer", "standard")
                .put("index.analysis.analyzer.custom2.position_increment_gap", "256")
                .put("path.home", createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_1_0_0,
                        Version.V_1_7_1))
                .build();
        AnalysisService analysisService = getAnalysisService(settings);

        Analyzer custom1 = analysisService.analyzer("custom1").analyzer();
        assertThat(custom1, instanceOf(CustomAnalyzer.class));
        assertThat(custom1.getPositionIncrementGap("custom1"), equalTo(128));

        Analyzer custom2 = analysisService.analyzer("custom2").analyzer();
        assertThat(custom2, instanceOf(CustomAnalyzer.class));
        assertThat(custom2.getPositionIncrementGap("custom2"), equalTo(256));
    }

    public void testWithBothSettings() {
        Settings settings = settingsBuilder()
                .put("index.analysis.analyzer.custom.tokenizer", "standard")
                .put("index.analysis.analyzer.custom.position_offset_gap", "128")
                .put("index.analysis.analyzer.custom.position_increment_gap", "256")
                .put("path.home", createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_1_0_0,
                        Version.V_1_7_1))
                .build();
        try {
            getAnalysisService(settings);
            fail("Analyzer has both position_offset_gap and position_increment_gap should fail");
        } catch (ProvisionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertThat(e.getCause().getMessage(), equalTo("Custom Analyzer [custom] defined both [position_offset_gap] and [position_increment_gap]" +
                    ", use only [position_increment_gap]"));
        }
    }

    public void testDeprecatedPositionOffsetGap() {
        Settings settings = settingsBuilder()
                .put("index.analysis.analyzer.custom.tokenizer", "standard")
                .put("index.analysis.analyzer.custom.position_offset_gap", "128")
                .put("path.home", createTempDir().toString())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        try {
            getAnalysisService(settings);
            fail("Analyzer should fail if it has position_offset_gap");
        } catch (ProvisionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertThat(e.getCause().getMessage(), equalTo("Option [position_offset_gap] in Custom Analyzer [custom] " +
                    "has been renamed, please use [position_increment_gap] instead."));
        }
    }
}
