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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.instanceOf;

public class KeepTypesFilterFactoryTests extends ESTokenStreamTestCase {

    private static final String BASE_SETTING = "index.analysis.filter.keep_numbers";

    public void testKeepTypesInclude() throws IOException {
        Settings.Builder settingsBuilder = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(BASE_SETTING + ".type", "keep_types")
                .putList(BASE_SETTING + "." + KeepTypesFilterFactory.KEEP_TYPES_KEY, new String[] { "<NUM>", "<SOMETHINGELSE>" });
        // either use default mode or set "include" mode explicitly
        if (random().nextBoolean()) {
            settingsBuilder.put(BASE_SETTING + "." + KeepTypesFilterFactory.KEEP_TYPES_MODE_KEY,
                    KeepTypesFilterFactory.KeepTypesMode.INCLUDE);
        }
        Settings settings = settingsBuilder.build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("keep_numbers");
        assertThat(tokenFilter, instanceOf(KeepTypesFilterFactory.class));
        String source = "Hello 123 world";
        String[] expected = new String[] { "123" };
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected, new int[] { 2 });
    }

    public void testKeepTypesExclude() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(BASE_SETTING + ".type", "keep_types")
                .putList(BASE_SETTING + "." + KeepTypesFilterFactory.KEEP_TYPES_KEY, new String[] { "<NUM>", "<SOMETHINGELSE>" })
                .put(BASE_SETTING + "." + KeepTypesFilterFactory.KEEP_TYPES_MODE_KEY, KeepTypesFilterFactory.KeepTypesMode.EXCLUDE).build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("keep_numbers");
        assertThat(tokenFilter, instanceOf(KeepTypesFilterFactory.class));
        String source = "Hello 123 world";
        String[] expected = new String[] { "Hello", "world" };
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected, new int[] { 1, 2 });
    }

    public void testKeepTypesException() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(BASE_SETTING + ".type", "keep_types")
                .putList(BASE_SETTING + "." + KeepTypesFilterFactory.KEEP_TYPES_KEY, new String[] { "<NUM>", "<SOMETHINGELSE>" })
                .put(BASE_SETTING + "." + KeepTypesFilterFactory.KEEP_TYPES_MODE_KEY, "bad_parameter").build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin()));
        assertEquals("`keep_types` tokenfilter mode can only be [include] or [exclude] but was [bad_parameter].", ex.getMessage());
    }
}
