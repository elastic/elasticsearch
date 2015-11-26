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

package org.elasticsearch.plugin.ingest.transport.simulate;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.plugin.ingest.PipelineStore;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.ingest.IngestDocument.MetaData.*;
import static org.elasticsearch.plugin.ingest.transport.simulate.SimulatePipelineRequest.Fields;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimulatePipelineRequestParsingTests extends ESTestCase {

    private PipelineStore store;

    @Before
    public void init() throws IOException {
        Pipeline pipeline = new Pipeline(SimulatePipelineRequest.SIMULATED_PIPELINE_ID, null, Collections.singletonList(mock(Processor.class)));
        Map<String, Processor.Factory> processorRegistry = new HashMap<>();
        processorRegistry.put("mock_processor", mock(Processor.Factory.class));
        store = mock(PipelineStore.class);
        when(store.get(SimulatePipelineRequest.SIMULATED_PIPELINE_ID)).thenReturn(pipeline);
        when(store.getProcessorFactoryRegistry()).thenReturn(processorRegistry);
    }

    public void testParseUsingPipelineStore() throws Exception {
        int numDocs = randomIntBetween(1, 10);

        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        List<Map<String, Object>> expectedDocs = new ArrayList<>();
        requestContent.put(Fields.DOCS, docs);
        for (int i = 0; i < numDocs; i++) {
            Map<String, Object> doc = new HashMap<>();
            String index = randomAsciiOfLengthBetween(1, 10);
            String type = randomAsciiOfLengthBetween(1, 10);
            String id = randomAsciiOfLengthBetween(1, 10);
            doc.put(INDEX.getFieldName(), index);
            doc.put(TYPE.getFieldName(), type);
            doc.put(ID.getFieldName(), id);
            String fieldName = randomAsciiOfLengthBetween(1, 10);
            String fieldValue = randomAsciiOfLengthBetween(1, 10);
            doc.put(Fields.SOURCE, Collections.singletonMap(fieldName, fieldValue));
            docs.add(doc);
            Map<String, Object> expectedDoc = new HashMap<>();
            expectedDoc.put(INDEX.getFieldName(), index);
            expectedDoc.put(TYPE.getFieldName(), type);
            expectedDoc.put(ID.getFieldName(), id);
            expectedDoc.put(Fields.SOURCE, Collections.singletonMap(fieldName, fieldValue));
            expectedDocs.add(expectedDoc);
        }

        SimulatePipelineRequest.Parsed actualRequest = SimulatePipelineRequest.parseWithPipelineId(SimulatePipelineRequest.SIMULATED_PIPELINE_ID, requestContent, false, store);
        assertThat(actualRequest.isVerbose(), equalTo(false));
        assertThat(actualRequest.getDocuments().size(), equalTo(numDocs));
        Iterator<Map<String, Object>> expectedDocsIterator = expectedDocs.iterator();
        for (IngestDocument ingestDocument : actualRequest.getDocuments()) {
            Map<String, Object> expectedDocument = expectedDocsIterator.next();
            assertThat(ingestDocument.getSource(), equalTo(expectedDocument.get(Fields.SOURCE)));
            assertThat(ingestDocument.getMetadata(INDEX), equalTo(expectedDocument.get(INDEX.getFieldName())));
            assertThat(ingestDocument.getMetadata(TYPE), equalTo(expectedDocument.get(TYPE.getFieldName())));
            assertThat(ingestDocument.getMetadata(ID), equalTo(expectedDocument.get(ID.getFieldName())));
        }

        assertThat(actualRequest.getPipeline().getId(), equalTo(SimulatePipelineRequest.SIMULATED_PIPELINE_ID));
        assertThat(actualRequest.getPipeline().getDescription(), nullValue());
        assertThat(actualRequest.getPipeline().getProcessors().size(), equalTo(1));
    }

    public void testParseWithProvidedPipeline() throws Exception {
        int numDocs = randomIntBetween(1, 10);

        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        List<Map<String, Object>> expectedDocs = new ArrayList<>();
        requestContent.put(Fields.DOCS, docs);
        for (int i = 0; i < numDocs; i++) {
            Map<String, Object> doc = new HashMap<>();
            String index = randomAsciiOfLengthBetween(1, 10);
            String type = randomAsciiOfLengthBetween(1, 10);
            String id = randomAsciiOfLengthBetween(1, 10);
            doc.put(INDEX.getFieldName(), index);
            doc.put(TYPE.getFieldName(), type);
            doc.put(ID.getFieldName(), id);
            String fieldName = randomAsciiOfLengthBetween(1, 10);
            String fieldValue = randomAsciiOfLengthBetween(1, 10);
            doc.put(Fields.SOURCE, Collections.singletonMap(fieldName, fieldValue));
            docs.add(doc);
            Map<String, Object> expectedDoc = new HashMap<>();
            expectedDoc.put(INDEX.getFieldName(), index);
            expectedDoc.put(TYPE.getFieldName(), type);
            expectedDoc.put(ID.getFieldName(), id);
            expectedDoc.put(Fields.SOURCE, Collections.singletonMap(fieldName, fieldValue));
            expectedDocs.add(expectedDoc);
        }

        Map<String, Object> pipelineConfig = new HashMap<>();
        List<Map<String, Object>> processors = new ArrayList<>();
        int numProcessors = randomIntBetween(1, 10);
        for (int i = 0; i < numProcessors; i++) {
            processors.add(Collections.singletonMap("mock_processor", Collections.emptyMap()));
        }
        pipelineConfig.put("processors", processors);
        requestContent.put(Fields.PIPELINE, pipelineConfig);

        SimulatePipelineRequest.Parsed actualRequest = SimulatePipelineRequest.parse(requestContent, false, store);
        assertThat(actualRequest.isVerbose(), equalTo(false));
        assertThat(actualRequest.getDocuments().size(), equalTo(numDocs));
        Iterator<Map<String, Object>> expectedDocsIterator = expectedDocs.iterator();
        for (IngestDocument ingestDocument : actualRequest.getDocuments()) {
            Map<String, Object> expectedDocument = expectedDocsIterator.next();
            assertThat(ingestDocument.getSource(), equalTo(expectedDocument.get(Fields.SOURCE)));
            assertThat(ingestDocument.getMetadata(INDEX), equalTo(expectedDocument.get(INDEX.getFieldName())));
            assertThat(ingestDocument.getMetadata(TYPE), equalTo(expectedDocument.get(TYPE.getFieldName())));
            assertThat(ingestDocument.getMetadata(ID), equalTo(expectedDocument.get(ID.getFieldName())));
        }

        assertThat(actualRequest.getPipeline().getId(), equalTo(SimulatePipelineRequest.SIMULATED_PIPELINE_ID));
        assertThat(actualRequest.getPipeline().getDescription(), nullValue());
        assertThat(actualRequest.getPipeline().getProcessors().size(), equalTo(numProcessors));
    }
}
