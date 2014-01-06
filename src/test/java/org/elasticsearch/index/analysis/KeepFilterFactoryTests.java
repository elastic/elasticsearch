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
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.instanceOf;

public class KeepFilterFactoryTests extends ElasticsearchTokenStreamTestCase {

    private static final String RESOURCE = "org/elasticsearch/index/analysis/keep_analysis.json";


    @Test
    public void testLoadWithoutSettings() {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("keep");
        Assert.assertNull(tokenFilter);
    }

    @Test
    public void testLoadOverConfiguredSettings() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.broken_keep_filter.type", "keep")
                .put("index.analysis.filter.broken_keep_filter.keep_words_path", "does/not/exists.txt")
                .put("index.analysis.filter.broken_keep_filter.keep_words", "[\"Hello\", \"worlD\"]")
                .build();
        try {
            AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            Assert.fail("path and array are configured");
        } catch (Exception e) {
            assertThat(e.getCause(), instanceOf(ElasticsearchIllegalArgumentException.class));
        }
    }

    @Test
    public void testKeepWordsPathSettings() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.non_broken_keep_filter.type", "keep")
                .put("index.analysis.filter.non_broken_keep_filter.keep_words_path", "does/not/exists.txt")
                .build();
        try {
            // test our none existing setup is picked up
            AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            fail("expected an exception due to non existent keep_words_path");
        } catch (Throwable e) {
            assertThat(e.getCause(), instanceOf(FailedToResolveConfigException.class));
        }

        settings = ImmutableSettings.settingsBuilder().put(settings)
                .put("index.analysis.filter.non_broken_keep_filter.keep_words", new String[]{"test"})
                .build();
        try {
            // test our none existing setup is picked up
            AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
            fail("expected an exception indicating that you can't use [keep_words_path] with [keep_words] ");
        } catch (Throwable e) {
            assertThat(e.getCause(), instanceOf(ElasticsearchIllegalArgumentException.class));
        }

    }

    @Test
    public void testCaseInsensitiveMapping() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_keep_filter");
        assertThat(tokenFilter, instanceOf(KeepWordFilterFactory.class));
        String source = "hello small world";
        String[] expected = new String[]{"hello", "world"};
        Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected, new int[]{1, 2});
    }

    @Test
    public void testCaseSensitiveMapping() throws IOException {
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromClassPath(RESOURCE);
        TokenFilterFactory tokenFilter = analysisService.tokenFilter("my_case_sensitive_keep_filter");
        assertThat(tokenFilter, instanceOf(KeepWordFilterFactory.class));
        String source = "Hello small world";
        String[] expected = new String[]{"Hello"};
        Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected, new int[]{1});
    }

}
