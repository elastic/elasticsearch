/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

public class LimitTokenCountFilterFactoryTests extends ESTokenStreamTestCase {
    public void testDefault() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.limit_default.type", "limit")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
        {
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("limit_default");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("limit");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
    }

    public void testSettings() throws IOException {
        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.limit_1.type", "limit")
                .put("index.analysis.filter.limit_1.max_token_count", 3)
                .put("index.analysis.filter.limit_1.consume_all_tokens", true)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("limit_1");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the", "quick", "brown" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.limit_1.type", "limit")
                .put("index.analysis.filter.limit_1.max_token_count", 3)
                .put("index.analysis.filter.limit_1.consume_all_tokens", false)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("limit_1");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the", "quick", "brown" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }

        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.limit_1.type", "limit")
                .put("index.analysis.filter.limit_1.max_token_count", 17)
                .put("index.analysis.filter.limit_1.consume_all_tokens", true)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            ESTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("limit_1");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the", "quick", "brown", "fox" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
    }

    private static ESTestCase.TestAnalysis createTestAnalysisFromSettings(Settings settings) throws IOException {
        return AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
    }

}
