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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.CompoundProcessor.ON_FAILURE_MESSAGE_FIELD;
import static org.elasticsearch.ingest.CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD;
import static org.elasticsearch.ingest.CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD;
import static org.elasticsearch.action.ingest.TrackingResultProcessor.decorate;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class TrackingResultProcessorTests extends ESTestCase {

    private IngestDocument ingestDocument;
    private List<SimulateProcessorResult> resultList;

    @Before
    public void init() {
        ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        resultList = new ArrayList<>();
    }

    public void testActualProcessor() throws Exception {
        TestProcessor actualProcessor = new TestProcessor(ingestDocument -> {});
        TrackingResultProcessor trackingProcessor = new TrackingResultProcessor(false, actualProcessor, resultList);
        trackingProcessor.execute(ingestDocument);

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(actualProcessor.getTag(), ingestDocument);

        assertThat(actualProcessor.getInvokedCounter(), equalTo(1));
        assertThat(resultList.size(), equalTo(1));

        assertThat(resultList.get(0).getIngestDocument(), equalTo(expectedResult.getIngestDocument()));
        assertThat(resultList.get(0).getFailure(), nullValue());
        assertThat(resultList.get(0).getProcessorTag(), equalTo(expectedResult.getProcessorTag()));
    }

    public void testActualCompoundProcessorWithoutOnFailure() throws Exception {
        RuntimeException exception = new RuntimeException("processor failed");
        TestProcessor testProcessor = new TestProcessor(ingestDocument -> {  throw exception; });
        CompoundProcessor actualProcessor = new CompoundProcessor(testProcessor);
        CompoundProcessor trackingProcessor = decorate(actualProcessor, resultList);

        try {
            trackingProcessor.execute(ingestDocument);
            fail("processor should throw exception");
        } catch (ElasticsearchException e) {
            assertThat(e.getRootCause().getMessage(), equalTo(exception.getMessage()));
        }

        SimulateProcessorResult expectedFirstResult = new SimulateProcessorResult(testProcessor.getTag(), ingestDocument);
        assertThat(testProcessor.getInvokedCounter(), equalTo(1));
        assertThat(resultList.size(), equalTo(1));
        assertThat(resultList.get(0).getIngestDocument(), nullValue());
        assertThat(resultList.get(0).getFailure(), equalTo(exception));
        assertThat(resultList.get(0).getProcessorTag(), equalTo(expectedFirstResult.getProcessorTag()));
    }

    public void testActualCompoundProcessorWithOnFailure() throws Exception {
        RuntimeException exception = new RuntimeException("fail");
        TestProcessor failProcessor = new TestProcessor("fail", "test", ingestDocument -> {  throw exception; });
        TestProcessor onFailureProcessor = new TestProcessor("success", "test", ingestDocument -> {});
        CompoundProcessor actualProcessor = new CompoundProcessor(false,
            Arrays.asList(new CompoundProcessor(false,
                Arrays.asList(failProcessor, onFailureProcessor),
                Arrays.asList(onFailureProcessor, failProcessor))),
                Arrays.asList(onFailureProcessor));
        CompoundProcessor trackingProcessor = decorate(actualProcessor, resultList);
        trackingProcessor.execute(ingestDocument);

        SimulateProcessorResult expectedFailResult = new SimulateProcessorResult(failProcessor.getTag(), ingestDocument);
        SimulateProcessorResult expectedSuccessResult = new SimulateProcessorResult(onFailureProcessor.getTag(), ingestDocument);

        assertThat(failProcessor.getInvokedCounter(), equalTo(2));
        assertThat(onFailureProcessor.getInvokedCounter(), equalTo(2));
        assertThat(resultList.size(), equalTo(4));

        assertThat(resultList.get(0).getIngestDocument(), nullValue());
        assertThat(resultList.get(0).getFailure(), equalTo(exception));
        assertThat(resultList.get(0).getProcessorTag(), equalTo(expectedFailResult.getProcessorTag()));

        Map<String, Object> metadata = resultList.get(1).getIngestDocument().getIngestMetadata();
        assertThat(metadata.get(ON_FAILURE_MESSAGE_FIELD), equalTo("fail"));
        assertThat(metadata.get(ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("test"));
        assertThat(metadata.get(ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("fail"));
        assertThat(resultList.get(1).getFailure(), nullValue());
        assertThat(resultList.get(1).getProcessorTag(), equalTo(expectedSuccessResult.getProcessorTag()));

        assertThat(resultList.get(2).getIngestDocument(), nullValue());
        assertThat(resultList.get(2).getFailure(), equalTo(exception));
        assertThat(resultList.get(2).getProcessorTag(), equalTo(expectedFailResult.getProcessorTag()));

        metadata = resultList.get(3).getIngestDocument().getIngestMetadata();
        assertThat(metadata.get(ON_FAILURE_MESSAGE_FIELD), equalTo("fail"));
        assertThat(metadata.get(ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("test"));
        assertThat(metadata.get(ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("fail"));
        assertThat(resultList.get(3).getFailure(), nullValue());
        assertThat(resultList.get(3).getProcessorTag(), equalTo(expectedSuccessResult.getProcessorTag()));
    }

    public void testActualCompoundProcessorWithIgnoreFailure() throws Exception {
        RuntimeException exception = new RuntimeException("processor failed");
        TestProcessor testProcessor = new TestProcessor(ingestDocument -> {  throw exception; });
        CompoundProcessor actualProcessor = new CompoundProcessor(true, Collections.singletonList(testProcessor),
            Collections.emptyList());
        CompoundProcessor trackingProcessor = decorate(actualProcessor, resultList);

        trackingProcessor.execute(ingestDocument);

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(testProcessor.getTag(), ingestDocument);
        assertThat(testProcessor.getInvokedCounter(), equalTo(1));
        assertThat(resultList.size(), equalTo(1));
        assertThat(resultList.get(0).getIngestDocument(), equalTo(expectedResult.getIngestDocument()));
        assertThat(resultList.get(0).getFailure(), sameInstance(exception));
        assertThat(resultList.get(0).getProcessorTag(), equalTo(expectedResult.getProcessorTag()));
    }
}
