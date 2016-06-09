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

import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class RenameProcessorTests extends ESTestCase {

    public void testRename() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        Object fieldValue = ingestDocument.getFieldValue(fieldName, Object.class);
        String newFieldName;
        do {
            newFieldName = RandomDocumentPicks.randomFieldName(random());
        } while (RandomDocumentPicks.canAddField(newFieldName, ingestDocument) == false || newFieldName.equals(fieldName));
        Processor processor = new RenameProcessor(randomAsciiOfLength(10), fieldName, newFieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(newFieldName, Object.class), equalTo(fieldValue));
    }

    public void testRenameArrayElement() throws Exception {
        Map<String, Object> document = new HashMap<>();
        List<String> list = new ArrayList<>();
        list.add("item1");
        list.add("item2");
        list.add("item3");
        document.put("list", list);
        List<Map<String, String>> one = new ArrayList<>();
        one.add(Collections.singletonMap("one", "one"));
        one.add(Collections.singletonMap("two", "two"));
        document.put("one", one);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Processor processor = new RenameProcessor(randomAsciiOfLength(10), "list.0", "item");
        processor.execute(ingestDocument);
        Object actualObject = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(actualObject, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> actualList = (List<String>) actualObject;
        assertThat(actualList.size(), equalTo(2));
        assertThat(actualList.get(0), equalTo("item2"));
        assertThat(actualList.get(1), equalTo("item3"));
        actualObject = ingestDocument.getSourceAndMetadata().get("item");
        assertThat(actualObject, instanceOf(String.class));
        assertThat(actualObject, equalTo("item1"));

        processor = new RenameProcessor(randomAsciiOfLength(10), "list.0", "list.3");
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[3] is out of bounds for array with length [2] as part of path [list.3]"));
            assertThat(actualList.size(), equalTo(2));
            assertThat(actualList.get(0), equalTo("item2"));
            assertThat(actualList.get(1), equalTo("item3"));
        }
    }

    public void testRenameNonExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new RenameProcessor(randomAsciiOfLength(10), fieldName, RandomDocumentPicks.randomFieldName(random()));
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] doesn't exist"));
        }
    }

    public void testRenameNewFieldAlreadyExists() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        Processor processor = new RenameProcessor(randomAsciiOfLength(10), RandomDocumentPicks.randomExistingFieldName(
                random(), ingestDocument), fieldName);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] already exists"));
        }
    }

    public void testRenameExistingFieldNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, null);
        String newFieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new RenameProcessor(randomAsciiOfLength(10), fieldName, newFieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(false));
        assertThat(ingestDocument.hasField(newFieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(newFieldName, Object.class), nullValue());
    }

    public void testRenameAtomicOperationSetFails() throws Exception {
        Map<String, Object> source = new HashMap<String, Object>() {
            @Override
            public Object put(String key, Object value) {
                if (key.equals("new_field")) {
                    throw new UnsupportedOperationException();
                }
                return super.put(key, value);
            }
        };
        source.put("list", Collections.singletonList("item"));

        IngestDocument ingestDocument = new IngestDocument(source, Collections.emptyMap());
        Processor processor = new RenameProcessor(randomAsciiOfLength(10), "list", "new_field");
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch(UnsupportedOperationException e) {
            //the set failed, the old field has not been removed
            assertThat(ingestDocument.getSourceAndMetadata().containsKey("list"), equalTo(true));
            assertThat(ingestDocument.getSourceAndMetadata().containsKey("new_field"), equalTo(false));
        }
    }

    public void testRenameAtomicOperationRemoveFails() throws Exception {
        Map<String, Object> source = new HashMap<String, Object>() {
            @Override
            public Object remove(Object key) {
                if (key.equals("list")) {
                    throw new UnsupportedOperationException();
                }
                return super.remove(key);
            }
        };
        source.put("list", Collections.singletonList("item"));

        IngestDocument ingestDocument = new IngestDocument(source, Collections.emptyMap());
        Processor processor = new RenameProcessor(randomAsciiOfLength(10), "list", "new_field");
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (UnsupportedOperationException e) {
            //the set failed, the old field has not been removed
            assertThat(ingestDocument.getSourceAndMetadata().containsKey("list"), equalTo(true));
            assertThat(ingestDocument.getSourceAndMetadata().containsKey("new_field"), equalTo(false));
        }
    }
}
