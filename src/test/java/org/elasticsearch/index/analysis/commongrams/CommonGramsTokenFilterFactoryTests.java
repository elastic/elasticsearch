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
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.instanceOf;
public class CommonGramsTokenFilterFactoryTests extends ElasticsearchTokenStreamTestCase {

    @Test
    public void testDefault() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.common_grams_default.type", "common_grams").build();

        try {
            AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            Assert.fail("[common_words] or [common_words_path] is set");
        } catch (Exception e) {
            assertThat(e.getCause(), instanceOf(ElasticsearchIllegalArgumentException.class));
        }
    }
    @Test
    public void testWithoutCommonWordsMatch() throws IOException {
        {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.common_grams_default.type", "common_grams")
                     .putArray("index.analysis.filter.common_grams_default.common_words", "chromosome", "protein")
                     .build();

            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            {
                TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_default");
                String source = "the quick brown is a fox Or noT";
                String[] expected = new String[] { "the", "quick", "brown", "is", "a", "fox", "Or", "noT" };
                Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
                assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
            }
        }

        {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.common_grams_default.type", "common_grams")
                     .put("index.analysis.filter.common_grams_default.query_mode", false)
                     .putArray("index.analysis.filter.common_grams_default.common_words", "chromosome", "protein")
                     .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            {
                TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_default");
                String source = "the quick brown is a fox Or noT";
                String[] expected = new String[] { "the", "quick", "brown", "is", "a", "fox", "Or", "noT" };
                Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
                assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
            }
        }
    }

    @Test
    public void testSettings() throws IOException {
        {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.common_grams_1.type", "common_grams")
                    .put("index.analysis.filter.common_grams_1.ignore_case", true)
                    .putArray("index.analysis.filter.common_grams_1.common_words", "the", "Or", "Not", "a", "is", "an", "they", "are")
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_1");
            String source = "the quick brown is a fox or noT";
            String[] expected = new String[] { "the", "the_quick", "quick", "brown", "brown_is", "is", "is_a", "a", "a_fox", "fox", "fox_or", "or", "or_noT", "noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.common_grams_2.type", "common_grams")
                    .put("index.analysis.filter.common_grams_2.ignore_case", false)
                    .putArray("index.analysis.filter.common_grams_2.common_words", "the", "Or", "noT", "a", "is", "an", "they", "are")
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_2");
            String source = "the quick brown is a fox or why noT";
            String[] expected = new String[] { "the", "the_quick", "quick", "brown", "brown_is", "is", "is_a", "a", "a_fox", "fox", "or", "why", "why_noT", "noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.common_grams_3.type", "common_grams")
                    .putArray("index.analysis.filter.common_grams_3.common_words", "the", "or", "not", "a", "is", "an", "they", "are")
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_3");
            String source = "the quick brown is a fox Or noT";
            String[] expected = new String[] { "the", "the_quick", "quick", "brown", "brown_is", "is", "is_a", "a", "a_fox", "fox", "Or", "noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
    }

    @Test
    public void testCommonGramsAnalysis() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder().loadFromClasspath("org/elasticsearch/index/analysis/commongrams/commongrams.json").build();
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
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.common_grams_1.type", "common_grams")
                    .put("index.analysis.filter.common_grams_1.query_mode", true)
                    .putArray("index.analysis.filter.common_grams_1.common_words", "the", "Or", "Not", "a", "is", "an", "they", "are")
                    .put("index.analysis.filter.common_grams_1.ignore_case", true)
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_1");
            String source = "the quick brown is a fox or noT";
            String[] expected = new String[] { "the_quick", "quick", "brown_is", "is_a", "a_fox", "fox_or", "or_noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.common_grams_2.type", "common_grams")
                    .put("index.analysis.filter.common_grams_2.query_mode", true)
                    .putArray("index.analysis.filter.common_grams_2.common_words", "the", "Or", "noT", "a", "is", "an", "they", "are")
                    .put("index.analysis.filter.common_grams_2.ignore_case", false)
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_2");
            String source = "the quick brown is a fox or why noT";
            String[] expected = new String[] { "the_quick", "quick", "brown_is", "is_a", "a_fox", "fox", "or", "why_noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.common_grams_3.type", "common_grams")
                    .put("index.analysis.filter.common_grams_3.query_mode", true)
                    .putArray("index.analysis.filter.common_grams_3.common_words", "the", "Or", "noT", "a", "is", "an", "they", "are")
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_3");
            String source = "the quick brown is a fox or why noT";
            String[] expected = new String[] { "the_quick", "quick", "brown_is", "is_a", "a_fox", "fox", "or", "why_noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.common_grams_4.type", "common_grams")
                    .put("index.analysis.filter.common_grams_4.query_mode", true)
                    .putArray("index.analysis.filter.common_grams_4.common_words", "the", "or", "not", "a", "is", "an", "they", "are")
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("common_grams_4");
            String source = "the quick brown is a fox Or noT";
            String[] expected = new String[] { "the_quick", "quick", "brown_is", "is_a", "a_fox", "fox", "Or", "noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
    }

    @Test
    public void testQueryModeCommonGramsAnalysis() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder().loadFromClasspath("org/elasticsearch/index/analysis/commongrams/commongrams_query_mode.json").build();
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

}
