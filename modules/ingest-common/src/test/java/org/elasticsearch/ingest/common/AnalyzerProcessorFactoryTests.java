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

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.MappingCharFilterFactory;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class AnalyzerProcessorFactoryTests extends ESTestCase {

    private final Settings settings = Settings.builder()
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();

    Environment environment = new Environment(settings);

    private static <T> AnalysisModule.AnalysisProvider<T> requriesAnalysisSettings(AnalysisModule.AnalysisProvider<T> provider) {
        return new AnalysisModule.AnalysisProvider<T>() {
            @Override
            public T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
                return provider.get(indexSettings, environment, name, settings);
            }
            @Override
            public boolean requiresAnalysisSettings() {
                return true;
            }
        };
    }

    private final AnalysisRegistry analysisRegistry = new AnalysisRegistry(environment,
        Collections.singletonMap("mapping", requriesAnalysisSettings(MappingCharFilterFactory::new)),
        emptyMap(),
        emptyMap(),
        emptyMap());

    public void testCreate() throws Exception {
        AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(environment, analysisRegistry);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("target_field", "field2");
        config.put("analyzer", "standard");
        String processorTag = randomAsciiOfLength(10);

        try(AnalyzerProcessor analyzerProcessor = factory.create(null, processorTag, config)) {
            assertThat(analyzerProcessor.getTag(), equalTo(processorTag));
            assertThat(analyzerProcessor.getField(), equalTo("field1"));
            assertThat(analyzerProcessor.getTargetField(), equalTo("field2"));
            assertThat(analyzerProcessor.getAnalyzer(), instanceOf(NamedAnalyzer.class));
            assertThat(((NamedAnalyzer)analyzerProcessor.getAnalyzer()).name(), equalTo("standard"));
        }
    }

    public void testCreateWithMinimalCustomAnalyzer() throws Exception {
        AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(environment, analysisRegistry);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tokenizer", "whitespace");

        String processorTag = randomAsciiOfLength(10);
        try (AnalyzerProcessor analyzerProcessor = factory.create(null, processorTag, config)) {
            assertThat(analyzerProcessor.getTag(), equalTo(processorTag));
            assertThat(analyzerProcessor.getField(), equalTo("field1"));
            assertThat(analyzerProcessor.getTargetField(), equalTo("field1"));
            assertThat(analyzerProcessor.getAnalyzer(), instanceOf(CustomAnalyzer.class));

            CustomAnalyzer customAnalyzer = (CustomAnalyzer) analyzerProcessor.getAnalyzer();
            assertThat(customAnalyzer.charFilters().length, equalTo(0));
            assertThat(customAnalyzer.tokenFilters().length, equalTo(0));
        }
    }


    public void testCreateWithCustomAnalyzer() throws Exception {
        AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(environment, analysisRegistry);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tokenizer", "keyword");

        Map<String, Object> truncateConfig = new HashMap<>();
        truncateConfig.put("type", "truncate");
        truncateConfig.put("length", 2);
        config.put("filter", Arrays.asList("lowercase", truncateConfig));

        Map<String, Object> charConfig = new HashMap<>();
        charConfig.put("type", "mapping");
        charConfig.put("mappings", Collections.singletonList("! => ?"));
        config.put("char_filter", Collections.singletonList(charConfig));

        String processorTag = randomAsciiOfLength(10);
        try (AnalyzerProcessor analyzerProcessor = factory.create(null, processorTag, config)) {
            assertThat(analyzerProcessor.getTag(), equalTo(processorTag));
            assertThat(analyzerProcessor.getField(), equalTo("field1"));
            assertThat(analyzerProcessor.getTargetField(), equalTo("field1"));
            assertThat(analyzerProcessor.getAnalyzer(), instanceOf(CustomAnalyzer.class));

            CustomAnalyzer customAnalyzer = (CustomAnalyzer) analyzerProcessor.getAnalyzer();
            assertThat(customAnalyzer.charFilters().length, equalTo(1));
            assertThat(customAnalyzer.tokenFilters().length, equalTo(2));
        }
    }

    public void testCreateNoFieldPresent() throws Exception {
        AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(environment, analysisRegistry);

        Map<String, Object> config = new HashMap<>();
        config.put("analyzer", "standard");
        String processorTag = randomAsciiOfLength(10);
        try {
            factory.create(null, processorTag, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoAnalyzerPresent() throws Exception {
        AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(environment, analysisRegistry);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        String processorTag = randomAsciiOfLength(10);
        try {
            factory.create(null, processorTag, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("missing analyzer definition"));
        }
    }

    public void testCreateWithUnknownAnalyzer() throws Exception {
        AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(environment, analysisRegistry);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("analyzer", "unknown_analyzer");
        String processorTag = randomAsciiOfLength(10);
        try {
            factory.create(null, processorTag, config);
            fail("factory create should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Unknown analyzer [unknown_analyzer]"));
        }
    }

    public void testCreateWithUnknownTokenizer() throws Exception {
        AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(environment, analysisRegistry);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tokenizer", "unknown");
        config.put("filter", Collections.singletonList("lowercase"));
        config.put("char_filter", Collections.singletonList("html_strip"));

        String processorTag = randomAsciiOfLength(10);
        try {
            factory.create(null, processorTag, config);
            fail("factory create should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("failed to find global tokenizer [unknown]"));
        }
    }

    public void testCreateWithBothAnalyzerAndTokenizer() throws Exception {
        AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(environment, analysisRegistry);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("analyzer", "standard");
        config.put("tokenizer", "keyword");

        String processorTag = randomAsciiOfLength(10);
        try {
            factory.create(null, processorTag, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("custom tokenizer or filters cannot be specified if a named analyzer is used"));
        }
    }

    public void testCreateWithUnknownTokenFilter() throws Exception {
        AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(environment, analysisRegistry);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tokenizer", "keyword");
        config.put("filter", Arrays.asList("lowercase", Collections.singletonMap("type", "unknown")));

        String processorTag = randomAsciiOfLength(10);
        try {
            factory.create(null, processorTag, config);
            fail("factory create should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("failed to find global token_filter [unknown]"));
        }
    }

    public void testCreateWithCharFilterWithoutType() throws Exception {
        AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(environment, analysisRegistry);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tokenizer", "keyword");
        config.put("char_filter", Arrays.asList("html_strip", Collections.singletonMap("no_type", "missing")));

        String processorTag = randomAsciiOfLength(10);
        try {
            factory.create(null, processorTag, config);
            fail("factory create should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Missing [type] setting for anonymous char_filter: {no_type=missing}"));
        }
    }

    public void testCreateWithNonStringTokenizerName() throws Exception {
        AnalyzerProcessor.Factory factory = new AnalyzerProcessor.Factory(environment, analysisRegistry);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("tokenizer", 42);

        String processorTag = randomAsciiOfLength(10);
        try {
            factory.create(null, processorTag, config);
            fail("factory create should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("failed to find or create tokenizer for [42]"));
        }
    }
}
