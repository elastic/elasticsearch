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

import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ingest.SimulatePipelineRequest.Fields;
import static org.elasticsearch.action.ingest.SimulatePipelineRequest.SIMULATED_PIPELINE_ID;
import static org.elasticsearch.ingest.IngestDocument.MetaData.ID;
import static org.elasticsearch.ingest.IngestDocument.MetaData.INDEX;
import static org.elasticsearch.ingest.IngestDocument.MetaData.ROUTING;
import static org.elasticsearch.ingest.IngestDocument.MetaData.TYPE;
import static org.elasticsearch.ingest.IngestDocument.MetaData.VERSION;
import static org.elasticsearch.ingest.IngestDocument.MetaData.VERSION_TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimulatePipelineRequestParsingTests extends ESTestCase {

    private IngestService ingestService;

    @Before
    public void init() throws IOException {
        TestProcessor processor = new TestProcessor(ingestDocument -> {});
        CompoundProcessor pipelineCompoundProcessor = new CompoundProcessor(processor);
        Pipeline pipeline = new Pipeline(SIMULATED_PIPELINE_ID, null, null, pipelineCompoundProcessor);
        Map<String, Processor.Factory> registry =
            Collections.singletonMap("mock_processor", (factories, tag, config) -> processor);
        ingestService = mock(IngestService.class);
        when(ingestService.getPipeline(SIMULATED_PIPELINE_ID)).thenReturn(pipeline);
        when(ingestService.getProcessorFactories()).thenReturn(registry);
    }

    public void testParseUsingPipelineStoreNoType() throws Exception {
        innerTestParseUsingPipelineStore(false);
    }

    public void testParseUsingPipelineStoreWithType() throws Exception {
        innerTestParseUsingPipelineStore(true);
    }

    private void innerTestParseUsingPipelineStore(boolean useExplicitType) throws Exception {
        int numDocs = randomIntBetween(1, 10);

        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        List<Map<String, Object>> expectedDocs = new ArrayList<>();
        requestContent.put(Fields.DOCS, docs);
        for (int i = 0; i < numDocs; i++) {
            Map<String, Object> doc = new HashMap<>();
            String index = randomAlphaOfLengthBetween(1, 10);
            String type = randomAlphaOfLengthBetween(1, 10);
            String id = randomAlphaOfLengthBetween(1, 10);
            doc.put(INDEX.getFieldName(), index);
            if (useExplicitType) {
                doc.put(TYPE.getFieldName(), type);
            }
            doc.put(ID.getFieldName(), id);
            String fieldName = randomAlphaOfLengthBetween(1, 10);
            String fieldValue = randomAlphaOfLengthBetween(1, 10);
            doc.put(Fields.SOURCE, Collections.singletonMap(fieldName, fieldValue));
            docs.add(doc);
            Map<String, Object> expectedDoc = new HashMap<>();
            expectedDoc.put(INDEX.getFieldName(), index);
            if (useExplicitType) {
                expectedDoc.put(TYPE.getFieldName(), type);
            } else {
                expectedDoc.put(TYPE.getFieldName(), "_doc");
            }
            expectedDoc.put(ID.getFieldName(), id);
            expectedDoc.put(Fields.SOURCE, Collections.singletonMap(fieldName, fieldValue));
            expectedDocs.add(expectedDoc);
        }

        SimulatePipelineRequest.Parsed actualRequest =
            SimulatePipelineRequest.parseWithPipelineId(SIMULATED_PIPELINE_ID, requestContent, false, ingestService);
        assertThat(actualRequest.isVerbose(), equalTo(false));
        assertThat(actualRequest.getDocuments().size(), equalTo(numDocs));
        Iterator<Map<String, Object>> expectedDocsIterator = expectedDocs.iterator();
        for (IngestDocument ingestDocument : actualRequest.getDocuments()) {
            Map<String, Object> expectedDocument = expectedDocsIterator.next();
            Map<IngestDocument.MetaData, Object> metadataMap = ingestDocument.extractMetadata();
            assertThat(metadataMap.get(INDEX), equalTo(expectedDocument.get(INDEX.getFieldName())));
            assertThat(metadataMap.get(TYPE), equalTo(expectedDocument.get(TYPE.getFieldName())));
            assertThat(metadataMap.get(ID), equalTo(expectedDocument.get(ID.getFieldName())));
            assertThat(ingestDocument.getSourceAndMetadata(), equalTo(expectedDocument.get(Fields.SOURCE)));
        }

        assertThat(actualRequest.getPipeline().getId(), equalTo(SIMULATED_PIPELINE_ID));
        assertThat(actualRequest.getPipeline().getDescription(), nullValue());
        assertThat(actualRequest.getPipeline().getProcessors().size(), equalTo(1));
        if (useExplicitType) {
            assertWarnings("[types removal] specifying _type in pipeline simulation requests is deprecated");
        }
    }

    public void testParseWithProvidedPipelineNoType() throws Exception {
        innerTestParseWithProvidedPipeline(false);
    }

    public void testParseWithProvidedPipelineWithType() throws Exception {
        innerTestParseWithProvidedPipeline(true);
    }

    private void innerTestParseWithProvidedPipeline(boolean useExplicitType) throws Exception {
        int numDocs = randomIntBetween(1, 10);

        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        List<Map<String, Object>> expectedDocs = new ArrayList<>();
        requestContent.put(Fields.DOCS, docs);
        for (int i = 0; i < numDocs; i++) {
            Map<String, Object> doc = new HashMap<>();
            Map<String, Object> expectedDoc = new HashMap<>();
            List<IngestDocument.MetaData> fields = Arrays.asList(INDEX, TYPE, ID, ROUTING, VERSION, VERSION_TYPE);
            for(IngestDocument.MetaData field : fields) {
                if (field == VERSION) {
                    Long value = randomLong();
                    doc.put(field.getFieldName(), value);
                    expectedDoc.put(field.getFieldName(), value);
                } else if (field == VERSION_TYPE) {
                    String value = VersionType.toString(
                        randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL, VersionType.EXTERNAL_GTE)
                    );
                    doc.put(field.getFieldName(), value);
                    expectedDoc.put(field.getFieldName(), value);
                } else if (field == TYPE) {
                    if (useExplicitType) {
                        String value = randomAlphaOfLengthBetween(1, 10);
                        doc.put(field.getFieldName(), value);
                        expectedDoc.put(field.getFieldName(), value);
                    } else {
                        expectedDoc.put(field.getFieldName(), "_doc");
                    }
                } else {
                    if (randomBoolean()) {
                        String value = randomAlphaOfLengthBetween(1, 10);
                        doc.put(field.getFieldName(), value);
                        expectedDoc.put(field.getFieldName(), value);
                    } else {
                        Integer value = randomIntBetween(1, 1000000);
                        doc.put(field.getFieldName(), value);
                        expectedDoc.put(field.getFieldName(), String.valueOf(value));
                    }
                }
            }
            String fieldName = randomAlphaOfLengthBetween(1, 10);
            String fieldValue = randomAlphaOfLengthBetween(1, 10);
            doc.put(Fields.SOURCE, Collections.singletonMap(fieldName, fieldValue));
            docs.add(doc);
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

        SimulatePipelineRequest.Parsed actualRequest = SimulatePipelineRequest.parse(requestContent, false, ingestService);
        assertThat(actualRequest.isVerbose(), equalTo(false));
        assertThat(actualRequest.getDocuments().size(), equalTo(numDocs));
        Iterator<Map<String, Object>> expectedDocsIterator = expectedDocs.iterator();
        for (IngestDocument ingestDocument : actualRequest.getDocuments()) {
            Map<String, Object> expectedDocument = expectedDocsIterator.next();
            Map<IngestDocument.MetaData, Object> metadataMap = ingestDocument.extractMetadata();
            assertThat(metadataMap.get(INDEX), equalTo(expectedDocument.get(INDEX.getFieldName())));
            assertThat(metadataMap.get(ID), equalTo(expectedDocument.get(ID.getFieldName())));
            assertThat(metadataMap.get(ROUTING), equalTo(expectedDocument.get(ROUTING.getFieldName())));
            assertThat(metadataMap.get(VERSION), equalTo(expectedDocument.get(VERSION.getFieldName())));
            assertThat(metadataMap.get(VERSION_TYPE), equalTo(expectedDocument.get(VERSION_TYPE.getFieldName())));
            assertThat(ingestDocument.getSourceAndMetadata(), equalTo(expectedDocument.get(Fields.SOURCE)));
        }

        assertThat(actualRequest.getPipeline().getId(), equalTo(SIMULATED_PIPELINE_ID));
        assertThat(actualRequest.getPipeline().getDescription(), nullValue());
        assertThat(actualRequest.getPipeline().getProcessors().size(), equalTo(numProcessors));
        if (useExplicitType) {
            assertWarnings("[types removal] specifying _type in pipeline simulation requests is deprecated");
        }
    }

    public void testNullPipelineId() {
        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        requestContent.put(Fields.DOCS, docs);
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> SimulatePipelineRequest.parseWithPipelineId(null, requestContent, false, ingestService));
        assertThat(e.getMessage(), equalTo("param [pipeline] is null"));
    }

    public void testNonExistentPipelineId() {
        String pipelineId = randomAlphaOfLengthBetween(1, 10);
        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        requestContent.put(Fields.DOCS, docs);
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> SimulatePipelineRequest.parseWithPipelineId(pipelineId, requestContent, false, ingestService));
        assertThat(e.getMessage(), equalTo("pipeline [" + pipelineId + "] does not exist"));
    }
}
