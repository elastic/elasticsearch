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

package org.elasticsearch.ingest.processor.add;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.equalTo;

public class AddProcessorTests extends ESTestCase {

    public void testAddExistingFields() throws IOException {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numFields = randomIntBetween(1, 5);
        Map<String, Object> fields = new HashMap<>();
        for (int i = 0; i < numFields; i++) {
            String fieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
            Object fieldValue = RandomDocumentPicks.randomFieldValue(random());
            fields.put(fieldName, fieldValue);
        }
        Processor processor = new AddProcessor(fields);
        processor.execute(ingestDocument);

        for (Map.Entry<String, Object> field : fields.entrySet()) {
            assertThat(ingestDocument.hasPropertyValue(field.getKey()), equalTo(true));
            assertThat(ingestDocument.getPropertyValue(field.getKey(), Object.class), equalTo(field.getValue()));
        }
    }

    public void testAddNewFields() throws IOException {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        //used to verify that there are no conflicts between subsequent fields going to be added
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        int numFields = randomIntBetween(1, 5);
        Map<String, Object> fields = new HashMap<>();
        for (int i = 0; i < numFields; i++) {
            Object fieldValue = RandomDocumentPicks.randomFieldValue(random());
            String fieldName = RandomDocumentPicks.addRandomField(random(), testIngestDocument, fieldValue);
            fields.put(fieldName, fieldValue);
        }
        Processor processor = new AddProcessor(fields);
        processor.execute(ingestDocument);
        for (Map.Entry<String, Object> field : fields.entrySet()) {
            assertThat(ingestDocument.hasPropertyValue(field.getKey()), equalTo(true));
            assertThat(ingestDocument.getPropertyValue(field.getKey(), Object.class), equalTo(field.getValue()));
        }
    }

    public void testAddFieldsTypeMismatch() throws IOException {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        ingestDocument.setPropertyValue("field", "value");
        Processor processor = new AddProcessor(Collections.singletonMap("field.inner", "value"));
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("cannot add field to parent [field] of type [java.lang.String], [java.util.Map] expected instead."));
        }
    }
}