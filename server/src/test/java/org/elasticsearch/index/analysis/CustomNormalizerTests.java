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

import org.apache.lucene.analysis.MockLowerCaseFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class CustomNormalizerTests extends ESTokenStreamTestCase {
    private static final AnalysisPlugin MOCK_ANALYSIS_PLUGIN = new MockAnalysisPlugin();

    public void testBasics() throws IOException {
        Settings settings = Settings.builder()
                .putList("index.analysis.normalizer.my_normalizer.filter", "lowercase")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, MOCK_ANALYSIS_PLUGIN);
        assertNull(analysis.indexAnalyzers.get("my_normalizer"));
        NamedAnalyzer normalizer = analysis.indexAnalyzers.getNormalizer("my_normalizer");
        assertNotNull(normalizer);
        assertEquals("my_normalizer", normalizer.name());
        assertTokenStreamContents(normalizer.tokenStream("foo", "Cet été-là"), new String[] {"cet été-là"});
        assertEquals(new BytesRef("cet été-là"), normalizer.normalize("foo", "Cet été-là"));

        normalizer = analysis.indexAnalyzers.getWhitespaceNormalizer("my_normalizer");
        assertNotNull(normalizer);
        assertEquals("my_normalizer", normalizer.name());
        assertTokenStreamContents(normalizer.tokenStream("foo", "Cet été-là"), new String[] {"cet", "été-là"});
        assertEquals(new BytesRef("cet été-là"), normalizer.normalize("foo", "Cet été-là"));
    }

    public void testUnknownType() {
        Settings settings = Settings.builder()
                .put("index.analysis.normalizer.my_normalizer.type", "foobar")
                .putList("index.analysis.normalizer.my_normalizer.filter", "lowercase", "asciifolding")
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
                () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, MOCK_ANALYSIS_PLUGIN));
        assertEquals("Custom normalizer [my_normalizer] cannot configure a tokenizer", e.getMessage());
    }

    public void testCharFilters() throws IOException {
        Settings settings = Settings.builder()
                .put("index.analysis.char_filter.my_mapping.type", "mock_char_filter")
                .putList("index.analysis.normalizer.my_normalizer.char_filter", "my_mapping")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, MOCK_ANALYSIS_PLUGIN);
        assertNull(analysis.indexAnalyzers.get("my_normalizer"));
        NamedAnalyzer normalizer = analysis.indexAnalyzers.getNormalizer("my_normalizer");
        assertNotNull(normalizer);
        assertEquals("my_normalizer", normalizer.name());
        assertTokenStreamContents(normalizer.tokenStream("foo", "abc acd"), new String[] {"zbc zcd"});
        assertEquals(new BytesRef("zbc"), normalizer.normalize("foo", "abc"));

        normalizer = analysis.indexAnalyzers.getWhitespaceNormalizer("my_normalizer");
        assertNotNull(normalizer);
        assertEquals("my_normalizer", normalizer.name());
        assertTokenStreamContents(normalizer.tokenStream("foo", "abc acd"), new String[] {"zbc", "zcd"});
        assertEquals(new BytesRef("zbc"), normalizer.normalize("foo", "abc"));
    }

    public void testIllegalFilters() throws IOException {
        Settings settings = Settings.builder()
                .putList("index.analysis.normalizer.my_normalizer.filter", "mock_forbidden")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, MOCK_ANALYSIS_PLUGIN));
        assertEquals("Custom normalizer [my_normalizer] may not use filter [mock_forbidden]", e.getMessage());
    }

    public void testIllegalCharFilters() throws IOException {
        Settings settings = Settings.builder()
                .putList("index.analysis.normalizer.my_normalizer.char_filter", "mock_forbidden")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, MOCK_ANALYSIS_PLUGIN));
        assertEquals("Custom normalizer [my_normalizer] may not use char filter [mock_forbidden]", e.getMessage());
    }

    private static class MockAnalysisPlugin implements AnalysisPlugin {
        @Override
        public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
            return singletonList(PreConfiguredTokenFilter.singleton("mock_forbidden", false, MockLowerCaseFilter::new));
        }

        @Override
        public List<PreConfiguredCharFilter> getPreConfiguredCharFilters() {
            return singletonList(PreConfiguredCharFilter.singleton("mock_forbidden", false, Function.identity()));
        }

        @Override
        public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
            return singletonMap("mock_char_filter", (indexSettings, env, name, settings) -> {
                class Factory implements NormalizingCharFilterFactory {
                    @Override
                    public String name() {
                        return name;
                    }
                    @Override
                    public Reader create(Reader reader) {
                        return new Reader() {
                            @Override
                            public int read(char[] cbuf, int off, int len) throws IOException {
                                int result = reader.read(cbuf, off, len);
                                for (int i = off; i < off + len; i++) {
                                    if (cbuf[i] == 'a') {
                                        cbuf[i] = 'z';
                                    }
                                }
                                return result;
                            }

                            @Override
                            public void close() throws IOException {
                                reader.close();
                            }
                        };
                    }
                }
                return new Factory();
            });
        }

        @Override
        public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
            return singletonMap("keyword", (indexSettings, environment, name, settings) ->
                TokenizerFactory.newFactory(name, () -> new MockTokenizer(MockTokenizer.KEYWORD, false)));
        }
    }
}
