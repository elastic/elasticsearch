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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.instanceOf;

public class KeepFilterFactoryTests extends ESTokenStreamTestCase {
    private static final String RESOURCE = "/org/elasticsearch/index/analysis/keep_analysis.json";

    public void testLoadWithoutSettings() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(createTempDir(), RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("keep");
        Assert.assertNull(tokenFilter);
    }

    public void testLoadOverConfiguredSettings() {
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.broken_keep_filter.type", "keep")
                .put("index.analysis.filter.broken_keep_filter.keep_words_path", "does/not/exists.txt")
                .put("index.analysis.filter.broken_keep_filter.keep_words", "[\"Hello\", \"worlD\"]")
                .build();
        try {
            AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            Assert.fail("path and array are configured");
        } catch (IllegalArgumentException e) {
        } catch (IOException e) {
            fail("expected IAE");
        }
    }

    public void testKeepWordsPathSettings() {
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.non_broken_keep_filter.type", "keep")
                .put("index.analysis.filter.non_broken_keep_filter.keep_words_path", "does/not/exists.txt")
                .build();
        try {
            // test our none existing setup is picked up
            AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            fail("expected an exception due to non existent keep_words_path");
        } catch (IllegalArgumentException e) {
        } catch (IOException e) {
            fail("expected IAE");
        }

        settings = Settings.builder().put(settings)
                .put("index.analysis.filter.non_broken_keep_filter.keep_words", new String[]{"test"})
                .build();
        try {
            // test our none existing setup is picked up
            AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            fail("expected an exception indicating that you can't use [keep_words_path] with [keep_words] ");
        } catch (IllegalArgumentException e) {
        } catch (IOException e) {
            fail("expected IAE");
        }

    }

    public void testCaseInsensitiveMapping() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(createTempDir(), RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_keep_filter");
        assertThat(tokenFilter, instanceOf(KeepWordFilterFactory.class));
        String source = "hello small world";
        String[] expected = new String[]{"hello", "world"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected, new int[]{1, 2});
    }

    public void testCaseSensitiveMapping() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(createTempDir(), RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_case_sensitive_keep_filter");
        assertThat(tokenFilter, instanceOf(KeepWordFilterFactory.class));
        String source = "Hello small world";
        String[] expected = new String[]{"Hello"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected, new int[]{1});
    }
}
