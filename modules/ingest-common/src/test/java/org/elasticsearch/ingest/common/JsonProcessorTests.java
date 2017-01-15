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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;

public class JsonProcessorTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testExecute() throws Exception {
        String processorTag = randomAsciiOfLength(3);
        String randomField = randomAsciiOfLength(3);
        String randomTargetField = randomAsciiOfLength(2);
        JsonProcessor jsonProcessor = new JsonProcessor(processorTag, randomField, randomTargetField, false);
        Map<String, Object> document = new HashMap<>();

        Map<String, Object> randomJsonMap = RandomDocumentPicks.randomSource(random());
        XContentBuilder builder = JsonXContent.contentBuilder().map(randomJsonMap);
        String randomJson = XContentHelper.convertToJson(builder.bytes(), false);
        document.put(randomField, randomJson);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);
        Map<String, Object> jsonified = ingestDocument.getFieldValue(randomTargetField, Map.class);
        assertIngestDocument(ingestDocument.getFieldValue(randomTargetField, Object.class), jsonified);
    }

    public void testInvalidJson() {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", "field", "target_field", false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", "invalid json");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> jsonProcessor.execute(ingestDocument));
        assertThat(exception.getCause().getCause().getMessage(), equalTo("Unrecognized token"
                + " 'invalid': was expecting ('true', 'false' or 'null')\n"
                + " at [Source: invalid json; line: 1, column: 8]"));
    }

    public void testFieldMissing() {
        JsonProcessor jsonProcessor = new JsonProcessor("tag", "field", "target_field", false);
        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> jsonProcessor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [field] not present as part of path [field]"));
    }

    @SuppressWarnings("unchecked")
    public void testAddToRoot() throws Exception {
        String processorTag = randomAsciiOfLength(3);
        String randomTargetField = randomAsciiOfLength(2);
        JsonProcessor jsonProcessor = new JsonProcessor(processorTag, "a", randomTargetField, true);
        Map<String, Object> document = new HashMap<>();

        String json = "{\"a\": 1, \"b\": 2}";
        document.put("a", json);
        document.put("c", "see");

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        jsonProcessor.execute(ingestDocument);

        Map<String, Object> expected = new HashMap<>();
        expected.put("a", 1);
        expected.put("b", 2);
        expected.put("c", "see");
        IngestDocument expectedIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), expected);

        assertIngestDocument(ingestDocument, expectedIngestDocument);
    }
}
