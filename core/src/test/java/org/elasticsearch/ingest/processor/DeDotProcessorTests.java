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

package org.elasticsearch.ingest.processor;

import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DeDotProcessorTests extends ESTestCase {

    public void testSimple() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("a.b", "hello world!");
        IngestDocument ingestDocument = new IngestDocument(source, Collections.emptyMap());
        String separator = randomUnicodeOfCodepointLengthBetween(1, 10);
        Processor processor = new DeDotProcessor(separator);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getSourceAndMetadata().get("a" + separator + "b" ), equalTo("hello world!"));
    }

    public void testSimpleMap() throws Exception {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> subField = new HashMap<>();
        subField.put("b.c", "hello world!");
        source.put("a", subField);
        IngestDocument ingestDocument = new IngestDocument(source, Collections.emptyMap());
        Processor processor = new DeDotProcessor("_");
        processor.execute(ingestDocument);

        IngestDocument expectedDocument = new IngestDocument(
            Collections.singletonMap("a", Collections.singletonMap("b_c", "hello world!")),
            Collections.emptyMap());
        assertThat(ingestDocument, equalTo(expectedDocument));
    }

    public void testSimpleList() throws Exception {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> subField = new HashMap<>();
        subField.put("b.c", "hello world!");
        source.put("a", Arrays.asList(subField));
        IngestDocument ingestDocument = new IngestDocument(source, Collections.emptyMap());
        Processor processor = new DeDotProcessor("_");
        processor.execute(ingestDocument);

        IngestDocument expectedDocument = new IngestDocument(
            Collections.singletonMap("a",
                Collections.singletonList(Collections.singletonMap("b_c", "hello world!"))),
            Collections.emptyMap());
        assertThat(ingestDocument, equalTo(expectedDocument));
    }
}
