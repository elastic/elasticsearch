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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AnalysisServiceTests extends ESTestCase {

    private static AnalyzerProvider analyzerProvider(final String name) {
        return new PreBuiltAnalyzerProvider(name, AnalyzerScope.INDEX, new EnglishAnalyzer());
    }

    public void testDefaultAnalyzers() throws IOException {
        Version version = VersionUtils.randomVersion(random());
        Settings settings = Settings
            .builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, version)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        AnalysisService analysisService = new AnalysisRegistry(null, new Environment(settings)).build(idxSettings);
        assertThat(analysisService.defaultIndexAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
        assertThat(analysisService.defaultSearchAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
        assertThat(analysisService.defaultSearchQuoteAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
    }

    public void testOverrideDefaultAnalyzer() throws IOException {
        Version version = VersionUtils.randomVersion(random());
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        AnalysisService analysisService = new AnalysisService(IndexSettingsModule.newIndexSettings("index", settings),
                Collections.singletonMap("default", analyzerProvider("default")),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertThat(analysisService.defaultIndexAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
        assertThat(analysisService.defaultSearchAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
        assertThat(analysisService.defaultSearchQuoteAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
    }

    public void testOverrideDefaultIndexAnalyzer() {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_5_0_0_alpha1, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        try {
            AnalysisService analysisService = new AnalysisService(IndexSettingsModule.newIndexSettings("index", settings),
                    Collections.singletonMap("default_index", new PreBuiltAnalyzerProvider("default_index", AnalyzerScope.INDEX, new EnglishAnalyzer())),
                    Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
            fail("Expected ISE");
        } catch (IllegalArgumentException e) {
            // expected
            assertTrue(e.getMessage().contains("[index.analysis.analyzer.default_index] is not supported"));
        }
    }

    public void testBackCompatOverrideDefaultIndexAnalyzer() {
        Version version = VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), VersionUtils.getPreviousVersion(Version.V_5_0_0_alpha1));
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        AnalysisService analysisService = new AnalysisService(IndexSettingsModule.newIndexSettings("index", settings),
                Collections.singletonMap("default_index", analyzerProvider("default_index")),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertThat(analysisService.defaultIndexAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
        assertThat(analysisService.defaultSearchAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
        assertThat(analysisService.defaultSearchQuoteAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
    }

    public void testOverrideDefaultSearchAnalyzer() {
        Version version = VersionUtils.randomVersion(random());
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        AnalysisService analysisService = new AnalysisService(IndexSettingsModule.newIndexSettings("index", settings),
                Collections.singletonMap("default_search", analyzerProvider("default_search")),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertThat(analysisService.defaultIndexAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
        assertThat(analysisService.defaultSearchAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
        assertThat(analysisService.defaultSearchQuoteAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
    }

    public void testBackCompatOverrideDefaultIndexAndSearchAnalyzer() {
        Version version = VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), VersionUtils.getPreviousVersion(Version.V_5_0_0_alpha1));
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        Map<String, AnalyzerProvider> analyzers = new HashMap<>();
        analyzers.put("default_index", analyzerProvider("default_index"));
        analyzers.put("default_search", analyzerProvider("default_search"));
        AnalysisService analysisService = new AnalysisService(IndexSettingsModule.newIndexSettings("index", settings),
                analyzers, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertThat(analysisService.defaultIndexAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
        assertThat(analysisService.defaultSearchAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
        assertThat(analysisService.defaultSearchQuoteAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
    }

    public void testConfigureCamelCaseTokenFilter() throws IOException {
        // tests a filter that
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("index.analysis.filter.wordDelimiter.type", "word_delimiter")
                .put("index.analysis.filter.wordDelimiter.split_on_numerics", false)
                .put("index.analysis.analyzer.custom_analyzer.tokenizer", "whitespace")
                .putArray("index.analysis.analyzer.custom_analyzer.filter", "lowercase", "wordDelimiter")
                .put("index.analysis.analyzer.custom_analyzer_1.tokenizer", "whitespace")
                .putArray("index.analysis.analyzer.custom_analyzer_1.filter", "lowercase", "word_delimiter").build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);
        AnalysisService analysisService = new AnalysisRegistry(null, new Environment(settings)).build(idxSettings);
        try (NamedAnalyzer custom_analyser = analysisService.analyzer("custom_analyzer")) {
            assertNotNull(custom_analyser);
            TokenStream tokenStream = custom_analyser.tokenStream("foo", "J2SE j2ee");
            tokenStream.reset();
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            List<String> token = new ArrayList<>();
            while(tokenStream.incrementToken()) {
                token.add(charTermAttribute.toString());
            }
            assertEquals(token.toString(), 2, token.size());
            assertEquals("j2se", token.get(0));
            assertEquals("j2ee", token.get(1));
        }

        try (NamedAnalyzer custom_analyser = analysisService.analyzer("custom_analyzer_1")) {
            assertNotNull(custom_analyser);
            TokenStream tokenStream = custom_analyser.tokenStream("foo", "J2SE j2ee");
            tokenStream.reset();
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            List<String> token = new ArrayList<>();
            while(tokenStream.incrementToken()) {
                token.add(charTermAttribute.toString());
            }
            assertEquals(token.toString(), 6, token.size());
            assertEquals("j", token.get(0));
            assertEquals("2", token.get(1));
            assertEquals("se", token.get(2));
            assertEquals("j", token.get(3));
            assertEquals("2", token.get(4));
            assertEquals("ee", token.get(5));
        }
    }

    public void testBuiltInAnalyzersAreCached() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);
        AnalysisService analysisService = new AnalysisRegistry(null, new Environment(settings)).build(idxSettings);
        AnalysisService otherAnalysisSergice = new AnalysisRegistry(null, new Environment(settings)).build(idxSettings);
        final int numIters = randomIntBetween(5, 20);
        for (int i = 0; i < numIters; i++) {
            PreBuiltAnalyzers preBuiltAnalyzers = RandomPicks.randomFrom(random(), PreBuiltAnalyzers.values());
            assertSame(analysisService.analyzer(preBuiltAnalyzers.name()), otherAnalysisSergice.analyzer(preBuiltAnalyzers.name()));
        }
    }

    public void testNoTypeOrTokenizerErrorMessage() throws IOException {
        Version version = VersionUtils.randomVersion(random());
        Settings settings = Settings
            .builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, version)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .putArray("index.analysis.analyzer.test_analyzer.filter", new String[] {"lowercase", "stop", "shingle"})
            .putArray("index.analysis.analyzer.test_analyzer.char_filter", new String[] {"html_strip"})
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new AnalysisRegistry(null, new Environment(settings)).build(idxSettings));
        assertThat(e.getMessage(), equalTo("analyzer [test_analyzer] must specify either an analyzer type, or a tokenizer"));
    }
}
