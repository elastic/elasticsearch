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

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AnalyzerProcessorTests extends ESTestCase {


    private final Settings settings = Settings.builder()
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();

    private final AnalysisRegistry analysisRegistry = new AnalysisRegistry(new Environment(settings),
        emptyMap(),
        emptyMap(),
        emptyMap(),
        emptyMap());

    private Analyzer getAnalyzer(String name) throws IOException {
        return analysisRegistry.getAnalyzer(name);
    }

    public void testAnalysis() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "This is a test.");
        try (Processor processor = new AnalyzerProcessor(randomAsciiOfLength(10), fieldName, fieldName, getAnalyzer("standard"), false)) {
            processor.execute(ingestDocument);
        }
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(Arrays.asList("this", "is", "a", "test")));
    }

    public void testAnalysisMultiValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        List<Object> multiValueField = new ArrayList<>();
        multiValueField.add("This is");
        multiValueField.add("a test.");
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, multiValueField);
        try (Processor processor = new AnalyzerProcessor(randomAsciiOfLength(10), fieldName, fieldName, getAnalyzer("standard"), false)) {
            processor.execute(ingestDocument);
        }
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(Arrays.asList("this", "is", "a", "test")));
    }

    public void testAnalysisWithTargetField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "This is a test.");
        String targetFieldName = randomAsciiOfLength(10);
        try (Processor processor = new AnalyzerProcessor(randomAsciiOfLength(10), fieldName, targetFieldName, getAnalyzer("standard"),
            false)) {
            processor.execute(ingestDocument);
        }
        assertThat(ingestDocument.getFieldValue(targetFieldName, List.class), equalTo(Arrays.asList("this", "is", "a", "test")));
    }

    public void testAnalysisFieldNotFound() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        try (Processor processor = new AnalyzerProcessor(randomAsciiOfLength(10), fieldName, fieldName, getAnalyzer("standard"), false)) {
            processor.execute(ingestDocument);
            fail("analyzer processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testAnalysisNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        try (Processor processor = new AnalyzerProcessor(randomAsciiOfLength(10), "field", "field", getAnalyzer("standard"), false)) {
            processor.execute(ingestDocument);
            fail("analyzer processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [field] is null, cannot be analyzed."));
        }
    }

    public void testAnalysisNonStringValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, randomInt());
        try (Processor processor = new AnalyzerProcessor(randomAsciiOfLength(10), fieldName, fieldName, getAnalyzer("standard"), false)) {
            processor.execute(ingestDocument);
            fail("analyzer processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] has type [java.lang.Integer] and cannot be analyzed"));
        }
    }

    public void testAnalysisAppendable() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("text", "This is a test.");
        IngestDocument ingestDocument = new IngestDocument(source, new HashMap<>());
        try (Processor processor = new AnalyzerProcessor(randomAsciiOfLength(10), "text", "text", getAnalyzer("standard"), false)) {
            processor.execute(ingestDocument);
        }
        @SuppressWarnings("unchecked")
        List<String> flags = (List<String>) ingestDocument.getFieldValue("text", List.class);
        assertThat(flags, equalTo(Arrays.asList("this", "is", "a", "test")));
        ingestDocument.appendFieldValue("text", "and this");
        assertThat(ingestDocument.getFieldValue("text", List.class), equalTo(Arrays.asList("this", "is", "a", "test", "and this")));
    }


}
