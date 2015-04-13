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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

public class LimitTokenCountFilterFactoryTests extends ElasticsearchTokenStreamTestCase {

    @Test
    public void testDefault() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.limit_default.type", "limit").build();
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
        {
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("limit_default");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("limit");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
    }

    @Test
    public void testSettings() throws IOException {
        {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.limit_1.type", "limit")
                    .put("index.analysis.filter.limit_1.max_token_count", 3).put("index.analysis.filter.limit_1.consume_all_tokens", true)
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("limit_1");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the", "quick", "brown" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.limit_1.type", "limit")
                    .put("index.analysis.filter.limit_1.max_token_count", 3).put("index.analysis.filter.limit_1.consume_all_tokens", false)
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("limit_1");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the", "quick", "brown" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }

        {
            Settings settings = ImmutableSettings.settingsBuilder().put("index.analysis.filter.limit_1.type", "limit")
                    .put("index.analysis.filter.limit_1.max_token_count", 17).put("index.analysis.filter.limit_1.consume_all_tokens", true)
                    .build();
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            TokenFilterFactory tokenFilter = analysisService.tokenFilter("limit_1");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the", "quick", "brown", "fox" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
    }

}
