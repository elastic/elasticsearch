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

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractStringProcessorTestCase extends ESTestCase {

    protected abstract AbstractStringProcessor newProcessor(String field, boolean ignoreMissing, String targetField);

    protected String modifyInput(String input) {
        return input;
    }

    protected abstract String expectedResult(String input);

    public void testProcessor() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldValue = RandomDocumentPicks.randomString(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, modifyInput(fieldValue));
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, String.class), equalTo(expectedResult(fieldValue)));
    }

    public void testFieldNotFound() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newProcessor(fieldName, false, fieldName);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
    }

    public void testFieldNotFoundWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newProcessor(fieldName, true, fieldName);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullValue() throws Exception {
        Processor processor = newProcessor("field", false, "field");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [field] is null, cannot process it."));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        Processor processor = newProcessor("field", true, "field");
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonStringValue() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newProcessor(fieldName, false, fieldName);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        ingestDocument.setFieldValue(fieldName, randomInt());
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [" + fieldName +
            "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
    }

    public void testNonStringValueWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = newProcessor(fieldName, true, fieldName);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        ingestDocument.setFieldValue(fieldName, randomInt());
        Exception e = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [" + fieldName +
            "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
    }

    public void testTargetField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        String fieldValue = RandomDocumentPicks.randomString(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, modifyInput(fieldValue));
        String targetFieldName = fieldName + "foo";
        Processor processor = newProcessor(fieldName, randomBoolean(), targetFieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetFieldName, String.class), equalTo(expectedResult(fieldValue)));
    }
}
