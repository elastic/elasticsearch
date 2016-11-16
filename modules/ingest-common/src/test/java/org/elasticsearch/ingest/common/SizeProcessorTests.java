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

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SizeProcessorTests extends ESTestCase {

    public void testSizeWithRandomDocument() throws Exception {
        String target = "_meta.size";
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Processor processor = new SizeProcessor(randomAsciiOfLength(10), target, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(target), equalTo(true));
        assertThat(ingestDocument.getFieldValue(target, Integer.class), greaterThan(0));
    }

    public void testSizeWithRandomDocumentAndOtherFieldName() throws Exception {
        String target = "b"+randomAsciiOfLength(10);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Processor processor = new SizeProcessor(randomAsciiOfLength(10), target, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(target), equalTo(true));
        assertThat(ingestDocument.getFieldValue(target, Integer.class), greaterThan(0));
    }

    public void testSizeWithKnownDocument() throws Exception {
        String target = "_meta.size";
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("foo", "bar"));
        Processor processor = new SizeProcessor(randomAsciiOfLength(10), target, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(target), equalTo(true));
        assertThat(ingestDocument.getFieldValue(target, Integer.class), equalTo(19));
    }

    public void testSizeWithKnownDocumentAndConsiderSize() throws Exception {
        String target = "_meta.size";
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("foo", "bar"));
        Processor processor = new SizeProcessor(randomAsciiOfLength(10), target, true);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(target), equalTo(true));
        assertThat(ingestDocument.getFieldValue(target, Integer.class),
            equalTo(19 + ",\n  \"_meta\" : {    \"size\" : 19\n  }\n".length()));
    }

    public void testSizeWithExistingMetaData() throws Exception {
        String target = "_meta.size";
        Map<String, Object> document = new HashMap<>();
        document.put("foo", "bar");
        Map<String, Object> meta = new HashMap<>();
        meta.put("source", "ingest");
        document.put("_meta", meta);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Processor processor = new SizeProcessor(randomAsciiOfLength(10), target, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(target), equalTo(true));
        assertThat(ingestDocument.getFieldValue(target, Integer.class), equalTo(62));
        assertThat(ingestDocument.hasField("_meta.source"), equalTo(true));
        assertThat(ingestDocument.getFieldValue("_meta.source", String.class), equalTo("ingest"));
    }

}
