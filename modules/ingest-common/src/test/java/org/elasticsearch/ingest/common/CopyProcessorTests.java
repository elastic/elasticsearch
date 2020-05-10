/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CopyProcessorTests extends ESTestCase {

    public void testCopyMap() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "target_field", false, false);
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> fieldValue = new HashMap<>();
        Map<String, Object> subFieldValue = new HashMap<>();
        subFieldValue.put("field2", 1);
        fieldValue.put("field1", subFieldValue);
        document.put("field", fieldValue);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        copyProcessor.execute(ingestDocument);
        assertThat(ingestDocument.hasField("target_field"), equalTo(true));
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(fieldValue));
    }

    public void testCopyArray() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "target_field", false, false);
        Map<String, Object> document = new HashMap<>();
        List<Boolean> fieldValue = Arrays.asList(true, true, false);
        document.put("field", fieldValue);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        copyProcessor.execute(ingestDocument);
        assertThat(ingestDocument.hasField("target_field"), equalTo(true));
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(fieldValue));
    }

    public void testCopyByteArray() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "target_field", false, false);
        Map<String, Object> document = new HashMap<>();
        byte[] fieldValue = new byte[] { 0, 1 };
        document.put("field", fieldValue);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        copyProcessor.execute(ingestDocument);
        assertThat(ingestDocument.hasField("target_field"), equalTo(true));
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(fieldValue));
    }

    public void testCopyString() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "target_field", false, false);
        Map<String, Object> document = new HashMap<>();
        String fieldValue = "bar";
        document.put("field", fieldValue);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        copyProcessor.execute(ingestDocument);
        assertThat(ingestDocument.hasField("target_field"), equalTo(true));
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(fieldValue));
    }

    public void testCopyInteger() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "target_field", false, false);
        Map<String, Object> document = new HashMap<>();
        int fieldValue = 1;
        document.put("field", fieldValue);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        copyProcessor.execute(ingestDocument);
        assertThat(ingestDocument.hasField("target_field"), equalTo(true));
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(fieldValue));
    }

    public void testCopyDouble() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "target_field", false, false);
        Map<String, Object> document = new HashMap<>();
        double fieldValue = 1.0;
        document.put("field", fieldValue);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        copyProcessor.execute(ingestDocument);
        assertThat(ingestDocument.hasField("target_field"), equalTo(true));
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(fieldValue));
    }

    public void testCopyBoolean() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "target_field", false, false);
        Map<String, Object> document = new HashMap<>();
        boolean fieldValue = true;
        document.put("field", fieldValue);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        copyProcessor.execute(ingestDocument);
        assertThat(ingestDocument.hasField("target_field"), equalTo(true));
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(fieldValue));
    }

    public void testCopyZonedDateTime() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "target_field", false, false);
        Map<String, Object> document = new HashMap<>();
        ZonedDateTime fieldValue = ZonedDateTime.now();
        document.put("field", fieldValue);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        copyProcessor.execute(ingestDocument);
        assertThat(ingestDocument.hasField("target_field"), equalTo(true));
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(fieldValue));
    }

    public void testCopyDate() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "target_field", false, false);
        Map<String, Object> document = new HashMap<>();
        Date fieldValue = new Date();
        document.put("field", fieldValue);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        copyProcessor.execute(ingestDocument);
        assertThat(ingestDocument.hasField("target_field"), equalTo(true));
        assertThat(ingestDocument.getFieldValue("target_field", Object.class), equalTo(fieldValue));
    }

    public void testCopyNull() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "target_field", false, false);
        Map<String, Object> document = new HashMap<>();
        Object fieldValue = null;
        document.put("field", fieldValue);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Exception exception = expectThrows(IllegalArgumentException.class, () -> copyProcessor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [field] is null, cannot be copied"));
    }

    public void testFieldMissing() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "target_field", false, false);
        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Exception exception = expectThrows(IllegalArgumentException.class, () -> copyProcessor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [field] not present as part of path [field]"));
    }

    public void testCopyWithIgnoreMissing() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "target_field", false, true);
        Map<String, Object> document = new HashMap<>();
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        copyProcessor.execute(ingestDocument);
        assertThat(ingestDocument.hasField("target_field"), equalTo(false));
    }

    public void testAddToRoot() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "", true, false);
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> fieldValue = new HashMap<>();
        Map<String, Object> subFieldValue = new HashMap<>();
        subFieldValue.put("field2", 1);
        fieldValue.put("field1", subFieldValue);
        document.put("field", fieldValue);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        copyProcessor.execute(ingestDocument);

        Map<String, Object> sourceAndMetadata = ingestDocument.getSourceAndMetadata();
        assertThat(sourceAndMetadata.get("field1"), equalTo(subFieldValue));
    }

    public void testAddNonMapToRoot() throws Exception {
        CopyProcessor copyProcessor = new CopyProcessor("tag", "field", "", true, false);
        Map<String, Object> document = new HashMap<>();
        document.put("field", true);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> copyProcessor.execute(ingestDocument));
        assertThat(exception.getMessage(), containsString("cannot add non-map fields to root of document"));
    }
}
