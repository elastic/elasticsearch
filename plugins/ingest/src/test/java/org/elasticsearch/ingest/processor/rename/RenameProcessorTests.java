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

package org.elasticsearch.ingest.processor.rename;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RenameProcessorTests extends ESTestCase {

    public void testRename() throws IOException {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numFields = randomIntBetween(1, 5);
        Map<String, String> fields = new HashMap<>();
        Map<String, Object> newFields = new HashMap<>();
        for (int i = 0; i < numFields; i++) {
            String fieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
            if (fields.containsKey(fieldName)) {
                continue;
            }
            String newFieldName;
            do {
                newFieldName = RandomDocumentPicks.randomFieldName(random());
            } while (RandomDocumentPicks.canAddField(newFieldName, ingestDocument) == false || newFields.containsKey(newFieldName));
            newFields.put(newFieldName, ingestDocument.getPropertyValue(fieldName, Object.class));
            fields.put(fieldName, newFieldName);
        }
        Processor processor = new RenameProcessor(fields);
        processor.execute(ingestDocument);
        for (Map.Entry<String, Object> entry : newFields.entrySet()) {
            assertThat(ingestDocument.getPropertyValue(entry.getKey(), Object.class), equalTo(entry.getValue()));
        }
    }

    public void testRenameNonExistingField() throws IOException {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Processor processor = new RenameProcessor(Collections.singletonMap(RandomDocumentPicks.randomFieldName(random()), RandomDocumentPicks.randomFieldName(random())));
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getSource().size(), equalTo(0));
    }

    public void testRenameExistingFieldNullValue() throws IOException {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setPropertyValue(fieldName, null);
        String newFieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new RenameProcessor(Collections.singletonMap(fieldName, newFieldName));
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasPropertyValue(fieldName), equalTo(false));
        assertThat(ingestDocument.hasPropertyValue(newFieldName), equalTo(true));
        assertThat(ingestDocument.getPropertyValue(newFieldName, Object.class), nullValue());
    }
}
