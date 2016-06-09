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

import org.elasticsearch.ingest.core.CompoundProcessor;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.core.TemplateService;
import org.elasticsearch.ingest.core.ValueSource;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ForEachProcessorTests extends ESTestCase {

    public void testExecute() throws Exception {
        List<String> values = new ArrayList<>();
        values.add("foo");
        values.add("bar");
        values.add("baz");
        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_type", "_id", null, null, null, null, Collections.singletonMap("values", values)
        );

        ForEachProcessor processor = new ForEachProcessor(
            "_tag", "values", Collections.singletonList(new UppercaseProcessor("_tag", "_value"))
        );
        processor.execute(ingestDocument);

        List<String> result = ingestDocument.getFieldValue("values", List.class);
        assertThat(result.get(0), equalTo("FOO"));
        assertThat(result.get(1), equalTo("BAR"));
        assertThat(result.get(2), equalTo("BAZ"));
    }

    public void testExecuteWithFailure() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_type", "_id", null, null, null, null, Collections.singletonMap("values", Arrays.asList("a", "b", "c"))
        );

        TestProcessor testProcessor = new TestProcessor(id -> {
            if ("c".equals(id.getFieldValue("_value", String.class))) {
                throw new RuntimeException("failure");
            }
        });
        ForEachProcessor processor = new ForEachProcessor("_tag", "values", Collections.singletonList(testProcessor));
        try {
            processor.execute(ingestDocument);
            fail("exception expected");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), equalTo("failure"));
        }
        assertThat(testProcessor.getInvokedCounter(), equalTo(3));
        assertThat(ingestDocument.getFieldValue("values", List.class), equalTo(Arrays.asList("a", "b", "c")));

        testProcessor = new TestProcessor(id -> {
            String value = id.getFieldValue("_value", String.class);
            if ("c".equals(value)) {
                throw new RuntimeException("failure");
            } else {
                id.setFieldValue("_value", value.toUpperCase(Locale.ROOT));
            }
        });
        Processor onFailureProcessor = new TestProcessor(ingestDocument1 -> {});
        processor = new ForEachProcessor(
            "_tag", "values",
            Collections.singletonList(new CompoundProcessor(false, Arrays.asList(testProcessor), Arrays.asList(onFailureProcessor)))
        );
        processor.execute(ingestDocument);
        assertThat(testProcessor.getInvokedCounter(), equalTo(3));
        assertThat(ingestDocument.getFieldValue("values", List.class), equalTo(Arrays.asList("A", "B", "c")));
    }

    public void testMetaDataAvailable() throws Exception {
        List<Map<String, Object>> values = new ArrayList<>();
        values.add(new HashMap<>());
        values.add(new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_type", "_id", null, null, null, null, Collections.singletonMap("values", values)
        );

        TestProcessor innerProcessor = new TestProcessor(id -> {
            id.setFieldValue("_value.index", id.getSourceAndMetadata().get("_index"));
            id.setFieldValue("_value.type", id.getSourceAndMetadata().get("_type"));
            id.setFieldValue("_value.id", id.getSourceAndMetadata().get("_id"));
        });
        ForEachProcessor processor = new ForEachProcessor("_tag", "values", Collections.singletonList(innerProcessor));
        processor.execute(ingestDocument);

        assertThat(innerProcessor.getInvokedCounter(), equalTo(2));
        assertThat(ingestDocument.getFieldValue("values.0.index", String.class), equalTo("_index"));
        assertThat(ingestDocument.getFieldValue("values.0.type", String.class), equalTo("_type"));
        assertThat(ingestDocument.getFieldValue("values.0.id", String.class), equalTo("_id"));
        assertThat(ingestDocument.getFieldValue("values.1.index", String.class), equalTo("_index"));
        assertThat(ingestDocument.getFieldValue("values.1.type", String.class), equalTo("_type"));
        assertThat(ingestDocument.getFieldValue("values.1.id", String.class), equalTo("_id"));
    }

    public void testRestOfTheDocumentIsAvailable() throws Exception {
        List<Map<String, Object>> values = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Map<String, Object> object = new HashMap<>();
            object.put("field", "value");
            values.add(object);
        }
        Map<String, Object> document = new HashMap<>();
        document.put("values", values);
        document.put("flat_values", new ArrayList<>());
        document.put("other", "value");
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", null, null, null, null, document);

        TemplateService ts = TestTemplateService.instance();
        ForEachProcessor processor = new ForEachProcessor(
                "_tag", "values", Arrays.asList(
                new AppendProcessor("_tag", ts.compile("flat_values"), ValueSource.wrap("value", ts)),
                new SetProcessor("_tag", ts.compile("_value.new_field"), (model) -> model.get("other")))
        );
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getFieldValue("values.0.new_field", String.class), equalTo("value"));
        assertThat(ingestDocument.getFieldValue("values.1.new_field", String.class), equalTo("value"));
        assertThat(ingestDocument.getFieldValue("values.2.new_field", String.class), equalTo("value"));
        assertThat(ingestDocument.getFieldValue("values.3.new_field", String.class), equalTo("value"));
        assertThat(ingestDocument.getFieldValue("values.4.new_field", String.class), equalTo("value"));

        List<String> flatValues = ingestDocument.getFieldValue("flat_values", List.class);
        assertThat(flatValues.size(), equalTo(5));
        assertThat(flatValues.get(0), equalTo("value"));
        assertThat(flatValues.get(1), equalTo("value"));
        assertThat(flatValues.get(2), equalTo("value"));
        assertThat(flatValues.get(3), equalTo("value"));
        assertThat(flatValues.get(4), equalTo("value"));
    }

    public void testRandom() throws Exception {
        int numProcessors = randomInt(8);
        List<Processor> processors = new ArrayList<>(numProcessors);
        for (int i = 0; i < numProcessors; i++) {
            processors.add(new Processor() {
                @Override
                public void execute(IngestDocument ingestDocument) throws Exception {
                    String existingValue = ingestDocument.getFieldValue("_value", String.class);
                    ingestDocument.setFieldValue("_value", existingValue + ".");
                }

                @Override
                public String getType() {
                    return null;
                }

                @Override
                public String getTag() {
                    return null;
                }
            });
        }
        int numValues = randomIntBetween(1, 32);
        List<String> values = new ArrayList<>(numValues);
        for (int i = 0; i < numValues; i++) {
            values.add("");
        }
        IngestDocument ingestDocument = new IngestDocument(
            "_index", "_type", "_id", null, null, null, null, Collections.singletonMap("values", values)
        );

        ForEachProcessor processor = new ForEachProcessor("_tag", "values", processors);
        processor.execute(ingestDocument);
        List<String> result = ingestDocument.getFieldValue("values", List.class);
        assertThat(result.size(), equalTo(numValues));

        String expectedString = "";
        for (int i = 0; i < numProcessors; i++) {
            expectedString = expectedString + ".";
        }

        for (String r : result) {
            assertThat(r, equalTo(expectedString));
        }
    }

}
