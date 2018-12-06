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

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.dissect.DissectException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;

import java.util.Collections;
import java.util.HashMap;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;

/**
 * Basic tests for the {@link DissectProcessor}. See the {@link org.elasticsearch.dissect.DissectParser} test suite for a comprehensive
 * set of dissect tests.
 */
public class DissectProcessorTests extends ESTestCase {

    public void testMatch() {
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", null, null, null,
            Collections.singletonMap("message", "foo,bar,baz"));
        DissectProcessor dissectProcessor = new DissectProcessor("", "message", "%{a},%{b},%{c}", "", true);
        dissectProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("a", String.class), equalTo("foo"));
        assertThat(ingestDocument.getFieldValue("b", String.class), equalTo("bar"));
        assertThat(ingestDocument.getFieldValue("c", String.class), equalTo("baz"));
    }

    public void testMatchOverwrite() {
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", null, null, null,
            MapBuilder.<String, Object>newMapBuilder()
                .put("message", "foo,bar,baz")
                .put("a", "willgetstompped")
                .map());
        assertThat(ingestDocument.getFieldValue("a", String.class), equalTo("willgetstompped"));
        DissectProcessor dissectProcessor = new DissectProcessor("", "message", "%{a},%{b},%{c}", "", true);
        dissectProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("a", String.class), equalTo("foo"));
        assertThat(ingestDocument.getFieldValue("b", String.class), equalTo("bar"));
        assertThat(ingestDocument.getFieldValue("c", String.class), equalTo("baz"));
    }

    public void testAdvancedMatch() {
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", null, null, null,
            Collections.singletonMap("message", "foo       bar,,,,,,,baz nope:notagain 😊 🐇 🙃"));
        DissectProcessor dissectProcessor =
            new DissectProcessor("", "message", "%{a->} %{*b->},%{&b} %{}:%{?skipme} %{+smile/2} 🐇 %{+smile/1}", "::::", true);
        dissectProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("a", String.class), equalTo("foo"));
        assertThat(ingestDocument.getFieldValue("bar", String.class), equalTo("baz"));
        expectThrows(IllegalArgumentException.class, () -> ingestDocument.getFieldValue("nope", String.class));
        expectThrows(IllegalArgumentException.class, () -> ingestDocument.getFieldValue("notagain", String.class));
        assertThat(ingestDocument.getFieldValue("smile", String.class), equalTo("🙃::::😊"));
    }

    public void testMiss() {
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", null, null, null,
            Collections.singletonMap("message", "foo:bar,baz"));
        DissectProcessor dissectProcessor = new DissectProcessor("", "message", "%{a},%{b},%{c}", "", true);
        DissectException e = expectThrows(DissectException.class, () -> dissectProcessor.execute(ingestDocument));
        assertThat(e.getMessage(), CoreMatchers.containsString("Unable to find match for dissect pattern"));
    }

    public void testNonStringValueWithIgnoreMissing() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new DissectProcessor("", fieldName, "%{a},%{b},%{c}", "", true);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        ingestDocument.setFieldValue(fieldName, randomInt());
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new DissectProcessor("", fieldName, "%{a},%{b},%{c}", "", true);
        IngestDocument originalIngestDocument = RandomDocumentPicks
            .randomIngestDocument(random(), Collections.singletonMap(fieldName, null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullValueWithOutIgnoreMissing() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new DissectProcessor("", fieldName, "%{a},%{b},%{c}", "", false);
        IngestDocument originalIngestDocument = RandomDocumentPicks
            .randomIngestDocument(random(), Collections.singletonMap(fieldName, null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
    }
}
