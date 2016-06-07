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
import org.elasticsearch.ingest.core.TemplateService;
import org.elasticsearch.ingest.core.ValueSource;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;

public class AppendProcessorTests extends ESTestCase {

    public void testAppendValuesToExistingList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Scalar scalar = randomFrom(Scalar.values());
        List<Object> list = new ArrayList<>();
        int size = randomIntBetween(0, 10);
        for (int i = 0; i < size; i++) {
            list.add(scalar.randomValue());
        }
        List<Object> checkList = new ArrayList<>(list);
        String field = RandomDocumentPicks.addRandomField(random(), ingestDocument, list);
        List<Object> values = new ArrayList<>();
        Processor appendProcessor;
        if (randomBoolean()) {
            Object value = scalar.randomValue();
            values.add(value);
            appendProcessor = createAppendProcessor(field, value);
        } else {
            int valuesSize = randomIntBetween(0, 10);
            for (int i = 0; i < valuesSize; i++) {
                values.add(scalar.randomValue());
            }
            appendProcessor = createAppendProcessor(field, values);
        }
        appendProcessor.execute(ingestDocument);
        Object fieldValue = ingestDocument.getFieldValue(field, Object.class);
        assertThat(fieldValue, sameInstance(list));
        assertThat(list.size(), equalTo(size + values.size()));
        for (int i = 0; i < size; i++) {
            assertThat(list.get(i), equalTo(checkList.get(i)));
        }
        for (int i = size; i < size + values.size(); i++) {
            assertThat(list.get(i), equalTo(values.get(i - size)));
        }
    }

    public void testAppendValuesToNonExistingList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String field = RandomDocumentPicks.randomFieldName(random());
        Scalar scalar = randomFrom(Scalar.values());
        List<Object> values = new ArrayList<>();
        Processor appendProcessor;
        if (randomBoolean()) {
            Object value = scalar.randomValue();
            values.add(value);
            appendProcessor = createAppendProcessor(field, value);
        } else {
            int valuesSize = randomIntBetween(0, 10);
            for (int i = 0; i < valuesSize; i++) {
                values.add(scalar.randomValue());
            }
            appendProcessor = createAppendProcessor(field, values);
        }
        appendProcessor.execute(ingestDocument);
        List list = ingestDocument.getFieldValue(field, List.class);
        assertThat(list, not(sameInstance(values)));
        assertThat(list, equalTo(values));
    }

    public void testConvertScalarToList() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Scalar scalar = randomFrom(Scalar.values());
        Object initialValue = scalar.randomValue();
        String field = RandomDocumentPicks.addRandomField(random(), ingestDocument, initialValue);
        List<Object> values = new ArrayList<>();
        Processor appendProcessor;
        if (randomBoolean()) {
            Object value = scalar.randomValue();
            values.add(value);
            appendProcessor = createAppendProcessor(field, value);
        } else {
            int valuesSize = randomIntBetween(0, 10);
            for (int i = 0; i < valuesSize; i++) {
                values.add(scalar.randomValue());
            }
            appendProcessor = createAppendProcessor(field, values);
        }
        appendProcessor.execute(ingestDocument);
        List fieldValue = ingestDocument.getFieldValue(field, List.class);
        assertThat(fieldValue.size(), equalTo(values.size() + 1));
        assertThat(fieldValue.get(0), equalTo(initialValue));
        for (int i = 1; i < values.size() + 1; i++) {
            assertThat(fieldValue.get(i), equalTo(values.get(i - 1)));
        }
    }

    public void testAppendMetadata() throws Exception {
        //here any metadata field value becomes a list, which won't make sense in most of the cases,
        // but support for append is streamlined like for set so we test it
        IngestDocument.MetaData randomMetaData = randomFrom(IngestDocument.MetaData.values());
        List<String> values = new ArrayList<>();
        Processor appendProcessor;
        if (randomBoolean()) {
            String value = randomAsciiOfLengthBetween(1, 10);
            values.add(value);
            appendProcessor = createAppendProcessor(randomMetaData.getFieldName(), value);
        } else {
            int valuesSize = randomIntBetween(0, 10);
            for (int i = 0; i < valuesSize; i++) {
                values.add(randomAsciiOfLengthBetween(1, 10));
            }
            appendProcessor = createAppendProcessor(randomMetaData.getFieldName(), values);
        }

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Object initialValue = ingestDocument.getSourceAndMetadata().get(randomMetaData.getFieldName());
        appendProcessor.execute(ingestDocument);
        List list = ingestDocument.getFieldValue(randomMetaData.getFieldName(), List.class);
        if (initialValue == null) {
            assertThat(list, equalTo(values));
        } else {
            assertThat(list.size(), equalTo(values.size() + 1));
            assertThat(list.get(0), equalTo(initialValue));
            for (int i = 1; i < list.size(); i++) {
                assertThat(list.get(i), equalTo(values.get(i - 1)));
            }
        }
    }

    private static Processor createAppendProcessor(String fieldName, Object fieldValue) {
        TemplateService templateService = TestTemplateService.instance();
        return new AppendProcessor(randomAsciiOfLength(10), templateService.compile(fieldName), ValueSource.wrap(fieldValue,
                templateService));
    }

    private enum Scalar {
        INTEGER {
            @Override
            Object randomValue() {
                return randomInt();
            }
        }, DOUBLE {
            @Override
            Object randomValue() {
                return randomDouble();
            }
        }, FLOAT {
            @Override
            Object randomValue() {
                return randomFloat();
            }
        }, BOOLEAN {
            @Override
            Object randomValue() {
                return randomBoolean();
            }
        }, STRING {
            @Override
            Object randomValue() {
                return randomAsciiOfLengthBetween(1, 10);
            }
        }, MAP {
            @Override
            Object randomValue() {
                int numItems = randomIntBetween(1, 10);
                Map<String, Object> map = new HashMap<>(numItems);
                for (int i = 0; i < numItems; i++) {
                    map.put(randomAsciiOfLengthBetween(1, 10), randomFrom(Scalar.values()).randomValue());
                }
                return map;
            }
        }, NULL {
            @Override
            Object randomValue() {
                return null;
            }
        };

        abstract Object randomValue();
    }
}
