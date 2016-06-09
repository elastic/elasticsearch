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

package org.elasticsearch.ingest;

import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;


public class GrokProcessorTests extends ESTestCase {

    public void testMatch() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "1");
        GrokProcessor processor = new GrokProcessor(randomAsciiOfLength(10), Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"), fieldName);
        processor.execute(doc);
        assertThat(doc.getFieldValue("one", String.class), equalTo("1"));
    }

    public void testNoMatch() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "23");
        GrokProcessor processor = new GrokProcessor(randomAsciiOfLength(10), Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"), fieldName);
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("Provided Grok expressions do not match field value: [23]"));
    }

    public void testMatchWithoutCaptures() throws Exception {
        String fieldName = "value";
        IngestDocument originalDoc = new IngestDocument(new HashMap<>(), new HashMap<>());
        originalDoc.setFieldValue(fieldName, fieldName);
        IngestDocument doc = new IngestDocument(originalDoc);
        GrokProcessor processor = new GrokProcessor(randomAsciiOfLength(10), Collections.emptyMap(),
            Collections.singletonList(fieldName), fieldName);
        processor.execute(doc);
        assertThat(doc, equalTo(originalDoc));
    }

    public void testNotStringField() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, 1);
        GrokProcessor processor = new GrokProcessor(randomAsciiOfLength(10), Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"), fieldName);
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
    }

    public void testMissingField() {
        String fieldName = "foo.bar";
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        GrokProcessor processor = new GrokProcessor(randomAsciiOfLength(10), Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"), fieldName);
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("field [foo] not present as part of path [foo.bar]"));
    }

    public void testMultiplePatternsWithMatchReturn() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "2");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONE", "1");
        patternBank.put("TWO", "2");
        patternBank.put("THREE", "3");
        GrokProcessor processor = new GrokProcessor(randomAsciiOfLength(10), patternBank,
            Arrays.asList("%{ONE:one}", "%{TWO:two}", "%{THREE:three}"), fieldName);
        processor.execute(doc);
        assertThat(doc.hasField("one"), equalTo(false));
        assertThat(doc.getFieldValue("two", String.class), equalTo("2"));
        assertThat(doc.hasField("three"), equalTo(false));
    }

    public void testSetMetadata() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "abc23");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONE", "1");
        patternBank.put("TWO", "2");
        patternBank.put("THREE", "3");
        GrokProcessor processor = new GrokProcessor(randomAsciiOfLength(10), patternBank,
            Arrays.asList("%{ONE:one}", "%{TWO:two}", "%{THREE:three}"), fieldName, true);
        processor.execute(doc);
        assertThat(doc.hasField("one"), equalTo(false));
        assertThat(doc.getFieldValue("two", String.class), equalTo("2"));
        assertThat(doc.hasField("three"), equalTo(false));
        assertThat(doc.getFieldValue("_ingest._grok_match_index", String.class), equalTo("1"));
    }

    public void testCombinedPatterns() {
        String combined;
        combined = GrokProcessor.combinePatterns(Arrays.asList(""), false);
        assertThat(combined, equalTo(""));
        combined = GrokProcessor.combinePatterns(Arrays.asList(""), true);
        assertThat(combined, equalTo(""));
        combined = GrokProcessor.combinePatterns(Arrays.asList("foo"), false);
        assertThat(combined, equalTo("foo"));
        combined = GrokProcessor.combinePatterns(Arrays.asList("foo"), true);
        assertThat(combined, equalTo("foo"));
        combined = GrokProcessor.combinePatterns(Arrays.asList("foo", "bar"), false);
        assertThat(combined, equalTo("(?:foo)|(?:bar)"));
        combined = GrokProcessor.combinePatterns(Arrays.asList("foo", "bar"), true);
        assertThat(combined, equalTo("(?<_ingest._grok_match_index.0>foo)|(?<_ingest._grok_match_index.1>bar)"));
    }
}
