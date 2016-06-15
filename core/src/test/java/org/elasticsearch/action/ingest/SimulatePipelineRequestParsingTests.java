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

package org.elasticsearch.action.ingest;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.ingest.PipelineStore;
import org.elasticsearch.ingest.ProcessorsRegistry;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.ingest.core.CompoundProcessor;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Pipeline;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ingest.SimulatePipelineRequest.Fields;
import static org.elasticsearch.action.ingest.SimulatePipelineRequest.SIMULATED_PIPELINE_ID;
import static org.elasticsearch.ingest.core.IngestDocument.MetaData.ID;
import static org.elasticsearch.ingest.core.IngestDocument.MetaData.INDEX;
import static org.elasticsearch.ingest.core.IngestDocument.MetaData.TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimulatePipelineRequestParsingTests extends ESTestCase {

    private PipelineStore store;

    @Before
    public void init() throws IOException {
        TestProcessor processor = new TestProcessor(ingestDocument -> {});
        CompoundProcessor pipelineCompoundProcessor = new CompoundProcessor(processor);
        Pipeline pipeline = new Pipeline(SIMULATED_PIPELINE_ID, null, pipelineCompoundProcessor);
        ProcessorsRegistry.Builder processorRegistryBuilder = new ProcessorsRegistry.Builder();
        processorRegistryBuilder.registerProcessor("mock_processor", ((registry) -> mock(Processor.Factory.class)));
        ProcessorsRegistry processorRegistry = processorRegistryBuilder.build(mock(ScriptService.class), mock(ClusterService.class));
        store = mock(PipelineStore.class);
        when(store.get(SIMULATED_PIPELINE_ID)).thenReturn(pipeline);
        when(store.getProcessorRegistry()).thenReturn(processorRegistry);
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

        SimulatePipelineRequest.Parsed actualRequest = SimulatePipelineRequest.parseWithPipelineId(SIMULATED_PIPELINE_ID, requestContent, false, store);
        assertThat(actualRequest.isVerbose(), equalTo(false));
        assertThat(actualRequest.getDocuments().size(), equalTo(numDocs));
        Iterator<Map<String, Object>> expectedDocsIterator = expectedDocs.iterator();
        for (IngestDocument ingestDocument : actualRequest.getDocuments()) {
            Map<String, Object> expectedDocument = expectedDocsIterator.next();
            Map<IngestDocument.MetaData, String> metadataMap = ingestDocument.extractMetadata();
            assertThat(metadataMap.get(INDEX), equalTo(expectedDocument.get(INDEX.getFieldName())));
            assertThat(metadataMap.get(TYPE), equalTo(expectedDocument.get(TYPE.getFieldName())));
            assertThat(metadataMap.get(ID), equalTo(expectedDocument.get(ID.getFieldName())));
            assertThat(ingestDocument.getSourceAndMetadata(), equalTo(expectedDocument.get(Fields.SOURCE)));
        }

        assertThat(actualRequest.getPipeline().getId(), equalTo(SIMULATED_PIPELINE_ID));
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
            Map<String, Object> processorConfig = new HashMap<>();
            List<Map<String, Object>> onFailureProcessors = new ArrayList<>();
            int numOnFailureProcessors = randomIntBetween(0, 1);
            for (int j = 0; j < numOnFailureProcessors; j++) {
                onFailureProcessors.add(Collections.singletonMap("mock_processor", Collections.emptyMap()));
            }
            if (numOnFailureProcessors > 0) {
                processorConfig.put("on_failure", onFailureProcessors);
            }
            processors.add(Collections.singletonMap("mock_processor", processorConfig));
        }
        pipelineConfig.put("processors", processors);

        List<Map<String, Object>> onFailureProcessors = new ArrayList<>();
        int numOnFailureProcessors = randomIntBetween(0, 1);
        for (int i = 0; i < numOnFailureProcessors; i++) {
            onFailureProcessors.add(Collections.singletonMap("mock_processor", Collections.emptyMap()));
        }
        if (numOnFailureProcessors > 0) {
            pipelineConfig.put("on_failure", onFailureProcessors);
        }

        requestContent.put(Fields.PIPELINE, pipelineConfig);

        SimulatePipelineRequest.Parsed actualRequest = SimulatePipelineRequest.parse(requestContent, false, store);
        assertThat(actualRequest.isVerbose(), equalTo(false));
        assertThat(actualRequest.getDocuments().size(), equalTo(numDocs));
        Iterator<Map<String, Object>> expectedDocsIterator = expectedDocs.iterator();
        for (IngestDocument ingestDocument : actualRequest.getDocuments()) {
            Map<String, Object> expectedDocument = expectedDocsIterator.next();
            Map<IngestDocument.MetaData, String> metadataMap = ingestDocument.extractMetadata();
            assertThat(metadataMap.get(INDEX), equalTo(expectedDocument.get(INDEX.getFieldName())));
            assertThat(metadataMap.get(TYPE), equalTo(expectedDocument.get(TYPE.getFieldName())));
            assertThat(metadataMap.get(ID), equalTo(expectedDocument.get(ID.getFieldName())));
            assertThat(ingestDocument.getSourceAndMetadata(), equalTo(expectedDocument.get(Fields.SOURCE)));
        }

        assertThat(actualRequest.getPipeline().getId(), equalTo(SIMULATED_PIPELINE_ID));
        assertThat(actualRequest.getPipeline().getDescription(), nullValue());
        assertThat(actualRequest.getPipeline().getProcessors().size(), equalTo(numProcessors));
    }

    public void testNullPipelineId() {
        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        requestContent.put(Fields.DOCS, docs);
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> SimulatePipelineRequest.parseWithPipelineId(null, requestContent, false, store));
        assertThat(e.getMessage(), equalTo("param [pipeline] is null"));
    }

    public void testNonExistentPipelineId() {
        String pipelineId = randomAsciiOfLengthBetween(1, 10);
        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        requestContent.put(Fields.DOCS, docs);
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> SimulatePipelineRequest.parseWithPipelineId(pipelineId, requestContent, false, store));
        assertThat(e.getMessage(), equalTo("pipeline [" + pipelineId + "] does not exist"));
    }
}
