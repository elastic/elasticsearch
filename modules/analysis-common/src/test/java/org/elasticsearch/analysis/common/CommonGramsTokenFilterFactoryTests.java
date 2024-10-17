/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

public class CommonGramsTokenFilterFactoryTests extends ESTokenStreamTestCase {
    public void testDefault() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.common_grams_default.type", "common_grams")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();

        try {
            AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
            Assert.fail("[common_words] or [common_words_path] is set");
        } catch (IllegalArgumentException e) {} catch (IOException e) {
            fail("expected IAE");
        }
    }

    public void testWithoutCommonWordsMatch() throws IOException {
        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.common_grams_default.type", "common_grams")
                .putList("index.analysis.filter.common_grams_default.common_words", "chromosome", "protein")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();

            ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            {
                TokenFilterFactory tokenFilter = analysis.tokenFilter.get("common_grams_default");
                String source = "the quick brown is a fox Or noT";
                String[] expected = new String[] { "the", "quick", "brown", "is", "a", "fox", "Or", "noT" };
                Tokenizer tokenizer = new WhitespaceTokenizer();
                tokenizer.setReader(new StringReader(source));
                assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
            }
        }

        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.common_grams_default.type", "common_grams")
                .put("index.analysis.filter.common_grams_default.query_mode", false)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .putList("index.analysis.filter.common_grams_default.common_words", "chromosome", "protein")
                .build();
            ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            {
                TokenFilterFactory tokenFilter = analysis.tokenFilter.get("common_grams_default");
                String source = "the quick brown is a fox Or noT";
                String[] expected = new String[] { "the", "quick", "brown", "is", "a", "fox", "Or", "noT" };
                Tokenizer tokenizer = new WhitespaceTokenizer();
                tokenizer.setReader(new StringReader(source));
                assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
            }
        }
    }

    public void testSettings() throws IOException {
        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.common_grams_1.type", "common_grams")
                .put("index.analysis.filter.common_grams_1.ignore_case", true)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .putList("index.analysis.filter.common_grams_1.common_words", "the", "Or", "Not", "a", "is", "an", "they", "are")
                .build();
            ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("common_grams_1");
            String source = "the quick brown is a fox or noT";
            String[] expected = new String[] {
                "the",
                "the_quick",
                "quick",
                "brown",
                "brown_is",
                "is",
                "is_a",
                "a",
                "a_fox",
                "fox",
                "fox_or",
                "or",
                "or_noT",
                "noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.common_grams_2.type", "common_grams")
                .put("index.analysis.filter.common_grams_2.ignore_case", false)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .putList("index.analysis.filter.common_grams_2.common_words", "the", "Or", "noT", "a", "is", "an", "they", "are")
                .build();
            ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("common_grams_2");
            String source = "the quick brown is a fox or why noT";
            String[] expected = new String[] {
                "the",
                "the_quick",
                "quick",
                "brown",
                "brown_is",
                "is",
                "is_a",
                "a",
                "" + "a_fox",
                "fox",
                "or",
                "why",
                "why_noT",
                "noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.common_grams_3.type", "common_grams")
                .putList("index.analysis.filter.common_grams_3.common_words", "the", "or", "not", "a", "is", "an", "they", "are")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("common_grams_3");
            String source = "the quick brown is a fox Or noT";
            String[] expected = new String[] {
                "the",
                "the_quick",
                "quick",
                "brown",
                "brown_is",
                "is",
                "is_a",
                "a",
                "a_fox",
                "fox",
                "Or",
                "noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
    }

    public void testCommonGramsAnalysis() throws IOException {
        String json = "/org/elasticsearch/analysis/common/commongrams.json";
        Settings settings = Settings.builder()
            .loadFromStream(json, getClass().getResourceAsStream(json), false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createHome())
            .build();
        {
            IndexAnalyzers indexAnalyzers = createTestAnalysisFromSettings(settings).indexAnalyzers;
            Analyzer analyzer = indexAnalyzers.get("commongramsAnalyzer").analyzer();
            String source = "the quick brown is a fox or not";
            String[] expected = new String[] {
                "the",
                "quick",
                "quick_brown",
                "brown",
                "brown_is",
                "is",
                "a",
                "a_fox",
                "fox",
                "fox_or",
                "or",
                "not" };
            assertTokenStreamContents(analyzer.tokenStream("test", source), expected);
        }
        {
            IndexAnalyzers indexAnalyzers = createTestAnalysisFromSettings(settings).indexAnalyzers;
            Analyzer analyzer = indexAnalyzers.get("commongramsAnalyzer_file").analyzer();
            String source = "the quick brown is a fox or not";
            String[] expected = new String[] {
                "the",
                "quick",
                "quick_brown",
                "brown",
                "brown_is",
                "is",
                "a",
                "a_fox",
                "fox",
                "fox_or",
                "or",
                "not" };
            assertTokenStreamContents(analyzer.tokenStream("test", source), expected);
        }
    }

    public void testQueryModeSettings() throws IOException {
        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.common_grams_1.type", "common_grams")
                .put("index.analysis.filter.common_grams_1.query_mode", true)
                .putList("index.analysis.filter.common_grams_1.common_words", "the", "Or", "Not", "a", "is", "an", "they", "are")
                .put("index.analysis.filter.common_grams_1.ignore_case", true)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("common_grams_1");
            String source = "the quick brown is a fox or noT";
            String[] expected = new String[] { "the_quick", "quick", "brown_is", "is_a", "a_fox", "fox_or", "or_noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.common_grams_2.type", "common_grams")
                .put("index.analysis.filter.common_grams_2.query_mode", true)
                .putList("index.analysis.filter.common_grams_2.common_words", "the", "Or", "noT", "a", "is", "an", "they", "are")
                .put("index.analysis.filter.common_grams_2.ignore_case", false)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("common_grams_2");
            String source = "the quick brown is a fox or why noT";
            String[] expected = new String[] { "the_quick", "quick", "brown_is", "is_a", "a_fox", "fox", "or", "why_noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.common_grams_3.type", "common_grams")
                .put("index.analysis.filter.common_grams_3.query_mode", true)
                .putList("index.analysis.filter.common_grams_3.common_words", "the", "Or", "noT", "a", "is", "an", "they", "are")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("common_grams_3");
            String source = "the quick brown is a fox or why noT";
            String[] expected = new String[] { "the_quick", "quick", "brown_is", "is_a", "a_fox", "fox", "or", "why_noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.common_grams_4.type", "common_grams")
                .put("index.analysis.filter.common_grams_4.query_mode", true)
                .putList("index.analysis.filter.common_grams_4.common_words", "the", "or", "not", "a", "is", "an", "they", "are")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("common_grams_4");
            String source = "the quick brown is a fox Or noT";
            String[] expected = new String[] { "the_quick", "quick", "brown_is", "is_a", "a_fox", "fox", "Or", "noT" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
    }

    public void testQueryModeCommonGramsAnalysis() throws IOException {
        String json = "/org/elasticsearch/analysis/common/commongrams_query_mode.json";
        Settings settings = Settings.builder()
            .loadFromStream(json, getClass().getResourceAsStream(json), false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createHome())
            .build();
        {
            IndexAnalyzers indexAnalyzers = createTestAnalysisFromSettings(settings).indexAnalyzers;
            Analyzer analyzer = indexAnalyzers.get("commongramsAnalyzer").analyzer();
            String source = "the quick brown is a fox or not";
            String[] expected = new String[] { "the", "quick_brown", "brown_is", "is", "a_fox", "fox_or", "or", "not" };
            assertTokenStreamContents(analyzer.tokenStream("test", source), expected);
        }
        {
            IndexAnalyzers indexAnalyzers = createTestAnalysisFromSettings(settings).indexAnalyzers;
            Analyzer analyzer = indexAnalyzers.get("commongramsAnalyzer_file").analyzer();
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

    private static ESTestCase.TestAnalysis createTestAnalysisFromSettings(Settings settings) throws IOException {
        return AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
    }

}
