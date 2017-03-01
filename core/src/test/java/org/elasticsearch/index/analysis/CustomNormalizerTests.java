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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;

public class CustomNormalizerTests extends ESTokenStreamTestCase {

    public void testBasics() throws IOException {
        Settings settings = Settings.builder()
                .putArray("index.analysis.normalizer.my_normalizer.filter", "lowercase", "asciifolding")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings);
        assertNull(analysis.indexAnalyzers.get("my_normalizer"));
        NamedAnalyzer normalizer = analysis.indexAnalyzers.getNormalizer("my_normalizer");
        assertNotNull(normalizer);
        assertEquals("my_normalizer", normalizer.name());
        assertTokenStreamContents(normalizer.tokenStream("foo", "Cet été-là"), new String[] {"cet ete-la"});
        assertEquals(new BytesRef("cet ete-la"), normalizer.normalize("foo", "Cet été-là"));
    }

    public void testUnknownType() {
        Settings settings = Settings.builder()
                .put("index.analysis.normalizer.my_normalizer.type", "foobar")
                .putArray("index.analysis.normalizer.my_normalizer.filter", "lowercase", "asciifolding")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings));
        assertEquals("Unknown normalizer type [foobar] for [my_normalizer]", e.getMessage());
    }

    public void testTokenizer() throws IOException {
        Settings settings = Settings.builder()
                .put("index.analysis.normalizer.my_normalizer.tokenizer", "keyword")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings));
        assertEquals("Custom normalizer [my_normalizer] cannot configure a tokenizer", e.getMessage());
    }

    public void testCharFilters() throws IOException {
        Settings settings = Settings.builder()
                .put("index.analysis.char_filter.my_mapping.type", "mapping")
                .putArray("index.analysis.char_filter.my_mapping.mappings", "a => z")
                .putArray("index.analysis.normalizer.my_normalizer.char_filter", "my_mapping")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings);
        assertNull(analysis.indexAnalyzers.get("my_normalizer"));
        NamedAnalyzer normalizer = analysis.indexAnalyzers.getNormalizer("my_normalizer");
        assertNotNull(normalizer);
        assertEquals("my_normalizer", normalizer.name());
        assertTokenStreamContents(normalizer.tokenStream("foo", "abc"), new String[] {"zbc"});
        assertEquals(new BytesRef("zbc"), normalizer.normalize("foo", "abc"));
    }

    public void testIllegalFilters() throws IOException {
        Settings settings = Settings.builder()
                .putArray("index.analysis.normalizer.my_normalizer.filter", "porter_stem")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings));
        assertEquals("Custom normalizer [my_normalizer] may not use filter [porter_stem]", e.getMessage());
    }

    public void testIllegalCharFilters() throws IOException {
        Settings settings = Settings.builder()
                .putArray("index.analysis.normalizer.my_normalizer.char_filter", "html_strip")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings));
        assertEquals("Custom normalizer [my_normalizer] may not use char filter [html_strip]", e.getMessage());
    }
}
