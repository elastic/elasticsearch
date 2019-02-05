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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ingest.SimulateProcessorResult;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.CompoundProcessor.ON_FAILURE_MESSAGE_FIELD;
import static org.elasticsearch.ingest.CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD;
import static org.elasticsearch.ingest.CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD;
import static org.elasticsearch.ingest.TrackingResultProcessor.decorate;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        TestProcessor testProcessor = new TestProcessor(ingestDocument -> { throw exception; });
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
        TestProcessor failProcessor = new TestProcessor("fail", "test", ingestDocument -> { throw exception; });
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
        TestProcessor testProcessor = new TestProcessor(ingestDocument -> { throw exception; });
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

    public void testActualCompoundProcessorWithFalseConditional() throws Exception {
        String key1 = randomAlphaOfLength(10);
        String key2 = randomAlphaOfLength(10);
        String key3 = randomAlphaOfLength(10);

        String scriptName = "conditionalScript";
        ScriptService scriptService = new ScriptService(Settings.builder().build(), Collections.singletonMap(Script.DEFAULT_SCRIPT_LANG,
            new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> false), Collections.emptyMap())),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );

        CompoundProcessor compoundProcessor = new CompoundProcessor(
            new TestProcessor(ingestDocument -> {ingestDocument.setFieldValue(key1, randomInt()); }),
            new ConditionalProcessor(
                randomAlphaOfLength(10),
                new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap()), scriptService,
                new TestProcessor(ingestDocument -> {ingestDocument.setFieldValue(key2, randomInt()); })),
            new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue(key3, randomInt()); }));

        CompoundProcessor trackingProcessor = decorate(compoundProcessor, resultList);
        trackingProcessor.execute(ingestDocument);
        SimulateProcessorResult expectedResult = new SimulateProcessorResult(compoundProcessor.getTag(), ingestDocument);

        //the step for key 2 is never executed due to conditional and thus not part of the result set
        assertThat(resultList.size(), equalTo(2));

        assertTrue(resultList.get(0).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(0).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(0).getIngestDocument().hasField(key3));

        assertTrue(resultList.get(1).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(1).getIngestDocument().hasField(key2));
        assertTrue(resultList.get(1).getIngestDocument().hasField(key3));

        assertThat(resultList.get(1).getIngestDocument(), equalTo(expectedResult.getIngestDocument()));
        assertThat(resultList.get(1).getFailure(), nullValue());
        assertThat(resultList.get(1).getProcessorTag(), nullValue());
    }

    public void testActualPipelineProcessor() throws Exception {
        String pipelineId = "pipeline1";
        IngestService ingestService = mock(IngestService.class);
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("name", pipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        String key1 = randomAlphaOfLength(10);
        String key2 = randomAlphaOfLength(10);
        String key3 = randomAlphaOfLength(10);

        Pipeline pipeline = new Pipeline(
            pipelineId, null, null, new CompoundProcessor(
            new TestProcessor(ingestDocument -> {ingestDocument.setFieldValue(key1, randomInt()); }),
            new TestProcessor(ingestDocument -> {ingestDocument.setFieldValue(key2, randomInt()); }),
            new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue(key3, randomInt()); }))
        );
        when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);

        PipelineProcessor pipelineProcessor = factory.create(Collections.emptyMap(), null, pipelineConfig);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, resultList);

        trackingProcessor.execute(ingestDocument);

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(actualProcessor.getTag(), ingestDocument);

        verify(ingestService,  Mockito.atLeast(1)).getPipeline(pipelineId);
        assertThat(resultList.size(), equalTo(3));

        assertTrue(resultList.get(0).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(0).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(0).getIngestDocument().hasField(key3));

        assertTrue(resultList.get(1).getIngestDocument().hasField(key1));
        assertTrue(resultList.get(1).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(1).getIngestDocument().hasField(key3));

        assertThat(resultList.get(2).getIngestDocument(), equalTo(expectedResult.getIngestDocument()));
        assertThat(resultList.get(2).getFailure(), nullValue());
        assertThat(resultList.get(2).getProcessorTag(), nullValue());
    }

    public void testActualPipelineProcessorWithTrueConditional() throws Exception {
        String pipelineId1 = "pipeline1";
        String pipelineId2 = "pipeline2";
        IngestService ingestService = mock(IngestService.class);
        Map<String, Object> pipelineConfig0 = new HashMap<>();
        pipelineConfig0.put("name", pipelineId1);
        Map<String, Object> pipelineConfig1 = new HashMap<>();
        pipelineConfig1.put("name", pipelineId1);
        Map<String, Object> pipelineConfig2 = new HashMap<>();
        pipelineConfig2.put("name", pipelineId2);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        String key1 = randomAlphaOfLength(10);
        String key2 = randomAlphaOfLength(10);
        String key3 = randomAlphaOfLength(10);

        String scriptName = "conditionalScript";

        ScriptService scriptService = new ScriptService(Settings.builder().build(), Collections.singletonMap(Script.DEFAULT_SCRIPT_LANG,
            new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> true), Collections.emptyMap())),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );

        Pipeline pipeline1 = new Pipeline(
            pipelineId1, null, null, new CompoundProcessor(
            new TestProcessor(ingestDocument -> {ingestDocument.setFieldValue(key1, randomInt()); }),
            new ConditionalProcessor(
                randomAlphaOfLength(10),
                new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap()), scriptService,
                factory.create(Collections.emptyMap(), null, pipelineConfig2)),
            new TestProcessor(ingestDocument -> {ingestDocument.setFieldValue(key3, randomInt()); })
        )
        );

        Pipeline pipeline2 = new Pipeline(
            pipelineId2, null, null, new CompoundProcessor(
                new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue(key2, randomInt()); })));

        when(ingestService.getPipeline(pipelineId1)).thenReturn(pipeline1);
        when(ingestService.getPipeline(pipelineId2)).thenReturn(pipeline2);

        PipelineProcessor pipelineProcessor = factory.create(Collections.emptyMap(), null, pipelineConfig0);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, resultList);

       trackingProcessor.execute(ingestDocument);


        SimulateProcessorResult expectedResult = new SimulateProcessorResult(actualProcessor.getTag(), ingestDocument);

        verify(ingestService, Mockito.atLeast(1)).getPipeline(pipelineId1);
        verify(ingestService, Mockito.atLeast(1)).getPipeline(pipelineId2);
        assertThat(resultList.size(), equalTo(3));

        assertTrue(resultList.get(0).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(0).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(0).getIngestDocument().hasField(key3));

        assertTrue(resultList.get(1).getIngestDocument().hasField(key1));
        assertTrue(resultList.get(1).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(1).getIngestDocument().hasField(key3));

        assertThat(resultList.get(2).getIngestDocument(), equalTo(expectedResult.getIngestDocument()));
        assertThat(resultList.get(2).getFailure(), nullValue());
        assertThat(resultList.get(2).getProcessorTag(), nullValue());
    }

    public void testActualPipelineProcessorWithFalseConditional() throws Exception {
        String pipelineId1 = "pipeline1";
        String pipelineId2 = "pipeline2";
        IngestService ingestService = mock(IngestService.class);
        Map<String, Object> pipelineConfig0 = new HashMap<>();
        pipelineConfig0.put("name", pipelineId1);
        Map<String, Object> pipelineConfig1 = new HashMap<>();
        pipelineConfig1.put("name", pipelineId1);
        Map<String, Object> pipelineConfig2 = new HashMap<>();
        pipelineConfig2.put("name", pipelineId2);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        String key1 = randomAlphaOfLength(10);
        String key2 = randomAlphaOfLength(10);
        String key3 = randomAlphaOfLength(10);

        String scriptName = "conditionalScript";

        ScriptService scriptService = new ScriptService(Settings.builder().build(), Collections.singletonMap(Script.DEFAULT_SCRIPT_LANG,
            new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> false), Collections.emptyMap())),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );

        Pipeline pipeline1 = new Pipeline(
            pipelineId1, null, null, new CompoundProcessor(
            new TestProcessor(ingestDocument -> {ingestDocument.setFieldValue(key1, randomInt()); }),
            new ConditionalProcessor(
                randomAlphaOfLength(10),
                new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap()), scriptService,
                factory.create(Collections.emptyMap(), null, pipelineConfig2)),
            new TestProcessor(ingestDocument -> {ingestDocument.setFieldValue(key3, randomInt()); })
        )
        );

        Pipeline pipeline2 = new Pipeline(
            pipelineId2, null, null, new CompoundProcessor(
            new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue(key2, randomInt()); })));

        when(ingestService.getPipeline(pipelineId1)).thenReturn(pipeline1);
        when(ingestService.getPipeline(pipelineId2)).thenReturn(pipeline2);

        PipelineProcessor pipelineProcessor = factory.create(Collections.emptyMap(), null, pipelineConfig0);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, resultList);

        trackingProcessor.execute(ingestDocument);


        SimulateProcessorResult expectedResult = new SimulateProcessorResult(actualProcessor.getTag(), ingestDocument);

        verify(ingestService, Mockito.atLeast(1)).getPipeline(pipelineId1);
        verify(ingestService, Mockito.never()).getPipeline(pipelineId2);
        assertThat(resultList.size(), equalTo(2));

        assertTrue(resultList.get(0).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(0).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(0).getIngestDocument().hasField(key3));

        assertTrue(resultList.get(1).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(1).getIngestDocument().hasField(key2));
        assertTrue(resultList.get(1).getIngestDocument().hasField(key3));

        assertThat(resultList.get(1).getIngestDocument(), equalTo(expectedResult.getIngestDocument()));
        assertThat(resultList.get(1).getFailure(), nullValue());
        assertThat(resultList.get(1).getProcessorTag(), nullValue());
    }

    public void testActualPipelineProcessorWithHandledFailure() throws Exception {
        RuntimeException exception = new RuntimeException("processor failed");

        String pipelineId = "pipeline1";
        IngestService ingestService = mock(IngestService.class);
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("name", pipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        String key1 = randomAlphaOfLength(10);
        String key2 = randomAlphaOfLength(10);
        String key3 = randomAlphaOfLength(10);

        Pipeline pipeline = new Pipeline(
            pipelineId, null, null, new CompoundProcessor(
            new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue(key1, randomInt()); }),
            new CompoundProcessor(
                false,
                Collections.singletonList(new TestProcessor(ingestDocument -> { throw exception; })),
                Collections.singletonList(new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue(key2, randomInt()); }))
            ),
            new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue(key3, randomInt()); }))
        );
        when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);

        PipelineProcessor pipelineProcessor = factory.create(Collections.emptyMap(), null, pipelineConfig);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, resultList);

        trackingProcessor.execute(ingestDocument);

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(actualProcessor.getTag(), ingestDocument);

        verify(ingestService, Mockito.atLeast(2)).getPipeline(pipelineId);
        assertThat(resultList.size(), equalTo(4));

        assertTrue(resultList.get(0).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(0).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(0).getIngestDocument().hasField(key3));

        //failed processor
        assertNull(resultList.get(1).getIngestDocument());
        assertThat(resultList.get(1).getFailure().getMessage(), equalTo(exception.getMessage()));

        assertTrue(resultList.get(2).getIngestDocument().hasField(key1));
        assertTrue(resultList.get(2).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(2).getIngestDocument().hasField(key3));

        assertThat(resultList.get(3).getIngestDocument(), equalTo(expectedResult.getIngestDocument()));
        assertThat(resultList.get(3).getFailure(), nullValue());
        assertThat(resultList.get(3).getProcessorTag(), nullValue());
    }

    public void testActualPipelineProcessorWithCycle() throws Exception {
        String pipelineId1 = "pipeline1";
        String pipelineId2 = "pipeline2";
        IngestService ingestService = mock(IngestService.class);
        Map<String, Object> pipelineConfig0 = new HashMap<>();
        pipelineConfig0.put("name", pipelineId1);
        Map<String, Object> pipelineConfig1 = new HashMap<>();
        pipelineConfig1.put("name", pipelineId1);
        Map<String, Object> pipelineConfig2 = new HashMap<>();
        pipelineConfig2.put("name", pipelineId2);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        Pipeline pipeline1 = new Pipeline(
            pipelineId1, null, null, new CompoundProcessor(factory.create(Collections.emptyMap(), null, pipelineConfig2)));

        Pipeline pipeline2 = new Pipeline(
            pipelineId2, null, null, new CompoundProcessor(factory.create(Collections.emptyMap(), null, pipelineConfig1)));

        when(ingestService.getPipeline(pipelineId1)).thenReturn(pipeline1);
        when(ingestService.getPipeline(pipelineId2)).thenReturn(pipeline2);

        PipelineProcessor pipelineProcessor = factory.create(Collections.emptyMap(), null, pipelineConfig0);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, resultList);

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> trackingProcessor.execute(ingestDocument));
        assertThat(exception.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(exception.getCause().getCause(), instanceOf(IllegalStateException.class));
        assertThat(exception.getMessage(), containsString("Cycle detected for pipeline: pipeline1"));
    }

    public void testActualPipelineProcessorRepeatedInvocation() throws Exception {
        String pipelineId = "pipeline1";
        IngestService ingestService = mock(IngestService.class);
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("name", pipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        String key1 = randomAlphaOfLength(10);
        PipelineProcessor pipelineProcessor = factory.create(Collections.emptyMap(), null, pipelineConfig);
        Pipeline pipeline = new Pipeline(
            pipelineId, null, null, new CompoundProcessor(
            new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue(key1, randomInt()); }))
        );
        when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);

        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor, pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, resultList);

        trackingProcessor.execute(ingestDocument);

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(actualProcessor.getTag(), ingestDocument);

        verify(ingestService,  Mockito.atLeast(2)).getPipeline(pipelineId);
        assertThat(resultList.size(), equalTo(2));

        assertThat(resultList.get(0).getIngestDocument(), not(equalTo(expectedResult.getIngestDocument())));
        assertThat(resultList.get(0).getFailure(), nullValue());
        assertThat(resultList.get(0).getProcessorTag(), nullValue());

        assertThat(resultList.get(1).getIngestDocument(), equalTo(expectedResult.getIngestDocument()));
        assertThat(resultList.get(1).getFailure(), nullValue());
        assertThat(resultList.get(1).getProcessorTag(), nullValue());

        //each invocation updates key1 with a random int
        assertNotEquals(resultList.get(0).getIngestDocument().getSourceAndMetadata().get(key1),
            resultList.get(1).getIngestDocument().getSourceAndMetadata().get(key1));
    }

}
