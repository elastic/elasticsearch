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

package org.elasticsearch.index.analysis.commongrams;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.instanceOf;
public class CommonGramsTokenFilterFactoryTests extends ESTokenStreamTestCase {

    @Test
    public void testDefault() throws IOException {
        Settings settings = Settings.settingsBuilder()
                                .put("index.analysis.filter.common_grams_default.type", "common_grams")
                                .put("path.home", createTempDir().toString())
                                .build();

        try {
            AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            Assert.fail("[common_words] or [common_words_path] is set");
        } catch (Exception e) {
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        }
    }
    @Test
    public void testWithoutCommonWordsMatch() throws IOException {
        {
            Settings settings = Settings.settingsBuilder().put("index.analysis.filter.common_grams_default.type", "common_grams")
                     .putArray("index.analysis.filter.common_grams_default.common_words", "chromosome", "protein")
                     .put("path.home", createTempDir().toString())
                     .build();

            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            {
                TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_default");
                String source = "the quick brown is a fox Or noT";
                String[] expected = new String[] { "the", "quick", "brown", "is", "a", "fox", "Or", "noT" };
                Tokenizer tokenizer = new WhitespaceTokenizer();
                tokenizer.setReader(new StringReader(source));
                assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
            }
        }

        {
            Settings settings = Settings.settingsBuilder().put("index.analysis.filter.common_grams_default.type", "common_grams")
                     .put("index.analysis.filter.common_grams_default.query_mode", false)
                     .put("path.home", createTempDir().toString())
                     .putArray("index.analysis.filter.common_grams_default.common_words", "chromosome", "protein")
                     .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            {
                TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_default");
                String source = "the quick brown is a fox Or noT";
                String[] expected = new String[] { "the", "quick", "brown", "is", "a", "fox", "Or", "noT" };
                Tokenizer tokenizer = new WhitespaceTokenizer();
                tokenizer.setReader(new StringReader(source));
                assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
            }
        }
    }

    @Test
    public void testSettings() throws IOException {
        {
            Settings settings = Settings.settingsBuilder().put("index.analysis.filter.common_grams_1.type", "common_grams")
                    .put("index.analysis.filter.common_grams_1.ignore_case", true)
                    .put("path.home", createTempDir().toString())
                    .putArray("index.analysis.filter.common_grams_1.common_words", "the", "Or", "Not", "a", "is", "an", "they", "are")
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_1");
            String source = "the quick brown is a fox or noT";
            String[] expected = new String[] { "the", "the_quick", "quick", "brown", "brown_is", "is", "is_a", "a", "a_fox", "fox", "fox_or", "or", "or_noT", "noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = Settings.settingsBuilder().put("index.analysis.filter.common_grams_2.type", "common_grams")
                    .put("index.analysis.filter.common_grams_2.ignore_case", false)
                    .put("path.home", createTempDir().toString())
                    .putArray("index.analysis.filter.common_grams_2.common_words", "the", "Or", "noT", "a", "is", "an", "they", "are")
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_2");
            String source = "the quick brown is a fox or why noT";
            String[] expected = new String[] { "the", "the_quick", "quick", "brown", "brown_is", "is", "is_a", "a", "a_fox", "fox", "or", "why", "why_noT", "noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = Settings.settingsBuilder().put("index.analysis.filter.common_grams_3.type", "common_grams")
                    .putArray("index.analysis.filter.common_grams_3.common_words", "the", "or", "not", "a", "is", "an", "they", "are")
                    .put("path.home", createTempDir().toString())
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_3");
            String source = "the quick brown is a fox Or noT";
            String[] expected = new String[] { "the", "the_quick", "quick", "brown", "brown_is", "is", "is_a", "a", "a_fox", "fox", "Or", "noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
    }

    @Test
    public void testCommonGramsAnalysis() throws IOException {
        String json = "/org/elasticsearch/index/analysis/commongrams/commongrams.json";
        Settings settings = Settings.settingsBuilder()
                     .loadFromStream(json, getClass().getResourceAsStream(json))
                     .put("path.home", createHome())
                     .build();
        {
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            Analyzer analyzer = analysisService.analyzer("commongramsAnalyzer").analyzer();
            String source = "the quick brown is a fox or not";
            String[] expected = new String[] { "the", "quick", "quick_brown", "brown", "brown_is", "is", "a", "a_fox", "fox", "fox_or", "or", "not" };
            assertTokenStreamContents(analyzer.tokenStream("test", source), expected);
        }
        {
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            Analyzer analyzer = analysisService.analyzer("commongramsAnalyzer_file").analyzer();
            String source = "the quick brown is a fox or not";
            String[] expected = new String[] { "the", "quick", "quick_brown", "brown", "brown_is", "is", "a", "a_fox", "fox", "fox_or", "or", "not" };
            assertTokenStreamContents(analyzer.tokenStream("test", source), expected);
        }
    }

    @Test
    public void testQueryModeSettings() throws IOException {
        {
            Settings settings = Settings.settingsBuilder().put("index.analysis.filter.common_grams_1.type", "common_grams")
                    .put("index.analysis.filter.common_grams_1.query_mode", true)
                    .putArray("index.analysis.filter.common_grams_1.common_words", "the", "Or", "Not", "a", "is", "an", "they", "are")
                    .put("index.analysis.filter.common_grams_1.ignore_case", true)
                    .put("path.home", createTempDir().toString())
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_1");
            String source = "the quick brown is a fox or noT";
            String[] expected = new String[] { "the_quick", "quick", "brown_is", "is_a", "a_fox", "fox_or", "or_noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = Settings.settingsBuilder().put("index.analysis.filter.common_grams_2.type", "common_grams")
                    .put("index.analysis.filter.common_grams_2.query_mode", true)
                    .putArray("index.analysis.filter.common_grams_2.common_words", "the", "Or", "noT", "a", "is", "an", "they", "are")
                    .put("index.analysis.filter.common_grams_2.ignore_case", false)
                    .put("path.home", createTempDir().toString())
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_2");
            String source = "the quick brown is a fox or why noT";
            String[] expected = new String[] { "the_quick", "quick", "brown_is", "is_a", "a_fox", "fox", "or", "why_noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = Settings.settingsBuilder().put("index.analysis.filter.common_grams_3.type", "common_grams")
                    .put("index.analysis.filter.common_grams_3.query_mode", true)
                    .putArray("index.analysis.filter.common_grams_3.common_words", "the", "Or", "noT", "a", "is", "an", "they", "are")
                    .put("path.home", createTempDir().toString())
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_3");
            String source = "the quick brown is a fox or why noT";
            String[] expected = new String[] { "the_quick", "quick", "brown_is", "is_a", "a_fox", "fox", "or", "why_noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = Settings.settingsBuilder().put("index.analysis.filter.common_grams_4.type", "common_grams")
                    .put("index.analysis.filter.common_grams_4.query_mode", true)
                    .putArray("index.analysis.filter.common_grams_4.common_words", "the", "or", "not", "a", "is", "an", "they", "are")
                    .put("path.home", createTempDir().toString())
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_4");
            String source = "the quick brown is a fox Or noT";
            String[] expected = new String[] { "the_quick", "quick", "brown_is", "is_a", "a_fox", "fox", "Or", "noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
    }

    @Test
    public void testQueryModeCommonGramsAnalysis() throws IOException {
        String json = "/org/elasticsearch/index/analysis/commongrams/commongrams_query_mode.json";
        Settings settings = Settings.settingsBuilder()
                .loadFromStream(json, getClass().getResourceAsStream(json))
            .put("path.home", createHome())
                .build();
        {
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            Analyzer analyzer = analysisService.analyzer("commongramsAnalyzer").analyzer();
            String source = "the quick brown is a fox or not";
            String[] expected = new String[] { "the", "quick_brown", "brown_is", "is", "a_fox", "fox_or", "or", "not" };
            assertTokenStreamContents(analyzer.tokenStream("test", source), expected);
        }
        {
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            Analyzer analyzer = analysisService.analyzer("commongramsAnalyzer_file").analyzer();
            String source = "the quick brown is a fox or not";
            String[] expected = new String[] { "the", "quick_brown", "brown_is", "is", "a_fox", "fox_or", "or", "not" };
            assertTokenStreamContents(analyzer.tokenStream("test", source), expected);
        }
    }

    private Path createHome() throws IOException {
        InputStream words = getClass().getResourceAsStream("common_words.txt");
        Path home = createTempDir();
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Files.copy(words, config.resolve("common_words.txt"));
        return home;
    }

}
