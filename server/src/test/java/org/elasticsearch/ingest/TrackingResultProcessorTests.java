/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.CompoundProcessor.ON_FAILURE_MESSAGE_FIELD;
import static org.elasticsearch.ingest.CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD;
import static org.elasticsearch.ingest.CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD;
import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.elasticsearch.ingest.PipelineProcessorTests.createIngestService;
import static org.elasticsearch.ingest.TrackingResultProcessor.decorate;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TrackingResultProcessorTests extends ESTestCase {

    private IngestDocument ingestDocument;
    private List<SimulateProcessorResult> resultList;

    @Before
    public void init() {
        ingestDocument = TestIngestDocument.emptyIngestDocument();
        resultList = new ArrayList<>();
    }

    public void testActualProcessor() throws Exception {
        TestProcessor actualProcessor = new TestProcessor(ingestDocument -> {});
        TrackingResultProcessor trackingProcessor = new TrackingResultProcessor(false, actualProcessor, null, resultList, true);
        trackingProcessor.execute(ingestDocument, (result, e) -> {});

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(
            actualProcessor.getType(),
            actualProcessor.getTag(),
            actualProcessor.getDescription(),
            ingestDocument,
            null
        );

        assertThat(actualProcessor.getInvokedCounter(), equalTo(1));
        assertThat(resultList.size(), equalTo(1));

        assertIngestDocument(resultList.get(0).getIngestDocument(), expectedResult.getIngestDocument());
        assertThat(resultList.get(0).getFailure(), nullValue());
        assertThat(resultList.get(0).getProcessorTag(), equalTo(expectedResult.getProcessorTag()));
    }

    public void testActualCompoundProcessorWithoutOnFailure() throws Exception {
        RuntimeException exception = new RuntimeException("processor failed");
        TestProcessor testProcessor = new TestProcessor(ingestDocument -> { throw exception; });
        CompoundProcessor actualProcessor = new CompoundProcessor(testProcessor);
        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);

        Exception[] holder = new Exception[1];
        trackingProcessor.execute(ingestDocument, (result, e) -> holder[0] = e);
        assertThat(((IngestProcessorException) holder[0]).getRootCause().getMessage(), equalTo(exception.getMessage()));

        SimulateProcessorResult expectedFirstResult = new SimulateProcessorResult(
            testProcessor.getType(),
            testProcessor.getTag(),
            actualProcessor.getDescription(),
            ingestDocument,
            null
        );
        assertThat(testProcessor.getInvokedCounter(), equalTo(1));
        assertThat(resultList.size(), equalTo(1));
        assertThat(resultList.get(0).getIngestDocument(), nullValue());
        assertThat(resultList.get(0).getFailure(), equalTo(exception));
        assertThat(resultList.get(0).getProcessorTag(), equalTo(expectedFirstResult.getProcessorTag()));
    }

    public void testActualCompoundProcessorWithOnFailure() throws Exception {
        RuntimeException exception = new RuntimeException("fail");
        TestProcessor failProcessor = new TestProcessor("fail", "test", null, exception);
        TestProcessor onFailureProcessor = new TestProcessor("success", "test", null, ingestDocument -> {});
        CompoundProcessor actualProcessor = new CompoundProcessor(
            false,
            List.of(new CompoundProcessor(false, List.of(failProcessor, onFailureProcessor), List.of(onFailureProcessor, failProcessor))),
            List.of(onFailureProcessor)
        );
        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);
        trackingProcessor.execute(ingestDocument, (result, e) -> {});

        SimulateProcessorResult expectedFailResult = new SimulateProcessorResult(
            failProcessor.getType(),
            failProcessor.getTag(),
            failProcessor.getDescription(),
            ingestDocument,
            null
        );
        SimulateProcessorResult expectedSuccessResult = new SimulateProcessorResult(
            onFailureProcessor.getType(),
            onFailureProcessor.getTag(),
            failProcessor.getDescription(),
            ingestDocument,
            null
        );

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

    public void testActualCompoundProcessorWithOnFailureAndTrueCondition() throws Exception {
        String scriptName = "conditionalScript";
        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Map.of(Script.DEFAULT_SCRIPT_LANG, new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Map.of(scriptName, ctx -> true), Map.of())),
            new HashMap<>(ScriptModule.CORE_CONTEXTS),
            () -> 1L
        );
        RuntimeException exception = new RuntimeException("fail");
        TestProcessor failProcessor = new TestProcessor("fail", "test", null, exception);
        ConditionalProcessor conditionalProcessor = new ConditionalProcessor(
            randomAlphaOfLength(10),
            null,
            new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Map.of()),
            scriptService,
            failProcessor
        );
        TestProcessor onFailureProcessor = new TestProcessor("success", "test", null, ingestDocument -> {});
        CompoundProcessor actualProcessor = new CompoundProcessor(false, List.of(conditionalProcessor), List.of(onFailureProcessor));
        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);
        trackingProcessor.execute(ingestDocument, (result, e) -> {});

        SimulateProcessorResult expectedFailResult = new SimulateProcessorResult(
            failProcessor.getType(),
            failProcessor.getTag(),
            failProcessor.getDescription(),
            ingestDocument,
            null
        );
        SimulateProcessorResult expectedSuccessResult = new SimulateProcessorResult(
            onFailureProcessor.getType(),
            onFailureProcessor.getTag(),
            onFailureProcessor.getDescription(),
            ingestDocument,
            null
        );

        assertThat(failProcessor.getInvokedCounter(), equalTo(1));
        assertThat(onFailureProcessor.getInvokedCounter(), equalTo(1));

        assertThat(resultList.size(), equalTo(2));

        assertThat(resultList.get(0).getIngestDocument(), nullValue());
        assertThat(resultList.get(0).getFailure(), equalTo(exception));
        assertThat(resultList.get(0).getProcessorTag(), equalTo(expectedFailResult.getProcessorTag()));
        assertThat(resultList.get(0).getConditionalWithResult().v1(), equalTo(scriptName));
        assertThat(resultList.get(0).getConditionalWithResult().v2(), is(Boolean.TRUE));

        Map<String, Object> metadata = resultList.get(1).getIngestDocument().getIngestMetadata();
        assertThat(metadata.get(ON_FAILURE_MESSAGE_FIELD), equalTo("fail"));
        assertThat(metadata.get(ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("test"));
        assertThat(metadata.get(ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("fail"));
        assertThat(resultList.get(1).getFailure(), nullValue());
        assertThat(resultList.get(1).getProcessorTag(), equalTo(expectedSuccessResult.getProcessorTag()));
    }

    public void testActualCompoundProcessorWithIgnoreFailure() throws Exception {
        RuntimeException exception = new RuntimeException("processor failed");
        TestProcessor testProcessor = new TestProcessor(ingestDocument -> { throw exception; });
        CompoundProcessor actualProcessor = new CompoundProcessor(true, List.of(testProcessor), List.of());
        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);

        trackingProcessor.execute(ingestDocument, (result, e) -> {});

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(
            testProcessor.getType(),
            testProcessor.getTag(),
            testProcessor.getDescription(),
            ingestDocument,
            null
        );
        assertThat(testProcessor.getInvokedCounter(), equalTo(1));
        assertThat(resultList.size(), equalTo(1));
        assertIngestDocument(resultList.get(0).getIngestDocument(), expectedResult.getIngestDocument());
        assertThat(resultList.get(0).getFailure(), sameInstance(exception));
        assertThat(resultList.get(0).getProcessorTag(), equalTo(expectedResult.getProcessorTag()));
    }

    public void testActualCompoundProcessorWithFalseConditional() throws Exception {
        String key1 = randomAlphaOfLength(10);
        String key2 = randomAlphaOfLength(10);
        String key3 = randomAlphaOfLength(10);

        String scriptName = "conditionalScript";
        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Map.of(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Map.of(scriptName, ctx -> false), Map.of())
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS),
            () -> 1L
        );

        CompoundProcessor compoundProcessor = new CompoundProcessor(new TestProcessor(ingestDocument -> {
            ingestDocument.setFieldValue(key1, randomInt());
        }),
            new ConditionalProcessor(
                randomAlphaOfLength(10),
                null,
                new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Map.of()),
                scriptService,
                new TestProcessor(ingestDocument -> {
                    ingestDocument.setFieldValue(key2, randomInt());
                })
            ),
            new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue(key3, randomInt()); })
        );

        CompoundProcessor trackingProcessor = decorate(compoundProcessor, null, resultList);
        trackingProcessor.execute(ingestDocument, (result, e) -> {});
        SimulateProcessorResult expectedResult = new SimulateProcessorResult(
            compoundProcessor.getType(),
            compoundProcessor.getTag(),
            compoundProcessor.getDescription(),
            ingestDocument,
            null
        );

        assertThat(resultList.size(), equalTo(3));

        assertTrue(resultList.get(0).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(0).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(0).getIngestDocument().hasField(key3));

        assertThat(resultList.get(1).getConditionalWithResult().v1(), equalTo(scriptName));
        assertThat(resultList.get(1).getConditionalWithResult().v2(), is(Boolean.FALSE));

        assertTrue(resultList.get(2).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(2).getIngestDocument().hasField(key2));
        assertTrue(resultList.get(2).getIngestDocument().hasField(key3));

        assertIngestDocument(resultList.get(2).getIngestDocument(), expectedResult.getIngestDocument());
        assertThat(resultList.get(2).getFailure(), nullValue());
        assertThat(resultList.get(2).getProcessorTag(), nullValue());
    }

    public void testActualPipelineProcessor() throws Exception {
        String pipelineId = "pipeline1";
        IngestService ingestService = createIngestService();
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("name", pipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        String key1 = randomAlphaOfLength(10);
        String key2 = randomAlphaOfLength(10);
        String key3 = randomAlphaOfLength(10);

        Pipeline pipeline = new Pipeline(pipelineId, null, null, null, new CompoundProcessor(new TestProcessor(ingestDocument -> {
            ingestDocument.setFieldValue(key1, randomInt());
        }), new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue(key2, randomInt()); }), new TestProcessor(ingestDocument -> {
            ingestDocument.setFieldValue(key3, randomInt());
        })));
        when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);

        PipelineProcessor pipelineProcessor = factory.create(Map.of(), null, null, pipelineConfig, null);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);

        trackingProcessor.execute(ingestDocument, (result, e) -> {});

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(
            actualProcessor.getType(),
            actualProcessor.getTag(),
            actualProcessor.getDescription(),
            ingestDocument,
            null
        );
        expectedResult.getIngestDocument().getIngestMetadata().put("pipeline", pipelineId);

        verify(ingestService, Mockito.atLeast(1)).getPipeline(pipelineId);

        assertThat(resultList.size(), equalTo(4));

        assertNull(resultList.get(0).getConditionalWithResult());
        assertThat(resultList.get(0).getType(), equalTo("pipeline"));

        assertTrue(resultList.get(1).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(1).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(1).getIngestDocument().hasField(key3));

        assertTrue(resultList.get(2).getIngestDocument().hasField(key1));
        assertTrue(resultList.get(2).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(2).getIngestDocument().hasField(key3));

        assertIngestDocument(resultList.get(3).getIngestDocument(), expectedResult.getIngestDocument());
        assertThat(resultList.get(3).getFailure(), nullValue());
        assertThat(resultList.get(3).getProcessorTag(), nullValue());
    }

    public void testActualPipelineProcessorWithTrueConditional() throws Exception {
        String pipelineId1 = "pipeline1";
        String pipelineId2 = "pipeline2";
        IngestService ingestService = createIngestService();
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

        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Map.of(Script.DEFAULT_SCRIPT_LANG, new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Map.of(scriptName, ctx -> true), Map.of())),
            new HashMap<>(ScriptModule.CORE_CONTEXTS),
            () -> 1L
        );

        Pipeline pipeline1 = new Pipeline(pipelineId1, null, null, null, new CompoundProcessor(new TestProcessor(ingestDocument -> {
            ingestDocument.setFieldValue(key1, randomInt());
        }),
            new ConditionalProcessor(
                randomAlphaOfLength(10),
                null,
                new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Map.of()),
                scriptService,
                factory.create(Map.of(), "pipeline1", null, pipelineConfig2, null)
            ),
            new TestProcessor(ingestDocument -> {
                ingestDocument.setFieldValue(key3, randomInt());
            })
        ));

        Pipeline pipeline2 = new Pipeline(pipelineId2, null, null, null, new CompoundProcessor(new TestProcessor(ingestDocument -> {
            ingestDocument.setFieldValue(key2, randomInt());
        })));

        when(ingestService.getPipeline(pipelineId1)).thenReturn(pipeline1);
        when(ingestService.getPipeline(pipelineId2)).thenReturn(pipeline2);

        PipelineProcessor pipelineProcessor = factory.create(Map.of(), "pipeline0", null, pipelineConfig0, null);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);

        trackingProcessor.execute(ingestDocument, (result, e) -> {});

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(
            actualProcessor.getType(),
            actualProcessor.getTag(),
            actualProcessor.getDescription(),
            ingestDocument,
            null
        );
        expectedResult.getIngestDocument().getIngestMetadata().put("pipeline", pipelineId1);

        verify(ingestService, Mockito.atLeast(1)).getPipeline(pipelineId1);
        verify(ingestService, Mockito.atLeast(1)).getPipeline(pipelineId2);

        assertThat(resultList.size(), equalTo(5));

        assertNull(resultList.get(0).getConditionalWithResult());
        assertThat(resultList.get(0).getType(), equalTo("pipeline"));
        assertThat(resultList.get(0).getProcessorTag(), equalTo("pipeline0"));

        assertTrue(resultList.get(1).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(1).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(1).getIngestDocument().hasField(key3));

        assertThat(resultList.get(2).getConditionalWithResult().v1(), equalTo(scriptName));
        assertThat(resultList.get(2).getConditionalWithResult().v2(), is(Boolean.TRUE));
        assertThat(resultList.get(2).getType(), equalTo("pipeline"));
        assertThat(resultList.get(2).getProcessorTag(), equalTo("pipeline1"));

        assertTrue(resultList.get(3).getIngestDocument().hasField(key1));
        assertTrue(resultList.get(3).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(3).getIngestDocument().hasField(key3));

        assertIngestDocument(resultList.get(4).getIngestDocument(), expectedResult.getIngestDocument());
        assertThat(resultList.get(4).getFailure(), nullValue());
        assertThat(resultList.get(4).getProcessorTag(), nullValue());
    }

    public void testActualPipelineProcessorWithFalseConditional() throws Exception {
        String pipelineId1 = "pipeline1";
        String pipelineId2 = "pipeline2";
        IngestService ingestService = createIngestService();
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

        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Map.of(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Map.of(scriptName, ctx -> false), Map.of())
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS),
            () -> 1L
        );

        Pipeline pipeline1 = new Pipeline(pipelineId1, null, null, null, new CompoundProcessor(new TestProcessor(ingestDocument -> {
            ingestDocument.setFieldValue(key1, randomInt());
        }),
            new ConditionalProcessor(
                randomAlphaOfLength(10),
                null,
                new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Map.of()),
                scriptService,
                factory.create(Map.of(), null, null, pipelineConfig2, null)
            ),
            new TestProcessor(ingestDocument -> {
                ingestDocument.setFieldValue(key3, randomInt());
            })
        ));

        Pipeline pipeline2 = new Pipeline(pipelineId2, null, null, null, new CompoundProcessor(new TestProcessor(ingestDocument -> {
            ingestDocument.setFieldValue(key2, randomInt());
        })));

        when(ingestService.getPipeline(pipelineId1)).thenReturn(pipeline1);
        when(ingestService.getPipeline(pipelineId2)).thenReturn(pipeline2);

        PipelineProcessor pipelineProcessor = factory.create(Map.of(), null, null, pipelineConfig0, null);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);

        trackingProcessor.execute(ingestDocument, (result, e) -> {});

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(
            actualProcessor.getType(),
            actualProcessor.getTag(),
            actualProcessor.getDescription(),
            ingestDocument,
            null
        );
        expectedResult.getIngestDocument().getIngestMetadata().put("pipeline", pipelineId1);

        verify(ingestService, Mockito.atLeast(1)).getPipeline(pipelineId1);
        verify(ingestService, Mockito.never()).getPipeline(pipelineId2);

        assertThat(resultList.size(), equalTo(4));

        assertNull(resultList.get(0).getConditionalWithResult());
        assertThat(resultList.get(0).getType(), equalTo("pipeline"));

        assertTrue(resultList.get(1).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(1).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(1).getIngestDocument().hasField(key3));

        assertThat(resultList.get(2).getConditionalWithResult().v1(), equalTo(scriptName));
        assertThat(resultList.get(2).getConditionalWithResult().v2(), is(Boolean.FALSE));

        assertIngestDocument(resultList.get(3).getIngestDocument(), expectedResult.getIngestDocument());
        assertThat(resultList.get(3).getFailure(), nullValue());
        assertThat(resultList.get(3).getProcessorTag(), nullValue());
    }

    public void testActualPipelineProcessorWithHandledFailure() throws Exception {
        RuntimeException exception = new RuntimeException("processor failed");

        String pipelineId = "pipeline1";
        IngestService ingestService = createIngestService();
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("name", pipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        String key1 = randomAlphaOfLength(10);
        String key2 = randomAlphaOfLength(10);
        String key3 = randomAlphaOfLength(10);

        Pipeline pipeline = new Pipeline(pipelineId, null, null, null, new CompoundProcessor(new TestProcessor(ingestDocument -> {
            ingestDocument.setFieldValue(key1, randomInt());
        }),
            new CompoundProcessor(
                false,
                List.of(new TestProcessor(ingestDocument -> { throw exception; })),
                List.of(new TestProcessor(ingestDocument -> {
                    ingestDocument.setFieldValue(key2, randomInt());
                }))
            ),
            new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue(key3, randomInt()); })
        ));
        when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);

        PipelineProcessor pipelineProcessor = factory.create(Map.of(), null, null, pipelineConfig, null);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);

        trackingProcessor.execute(ingestDocument, (result, e) -> {});

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(
            actualProcessor.getType(),
            actualProcessor.getTag(),
            actualProcessor.getDescription(),
            ingestDocument,
            null
        );
        expectedResult.getIngestDocument().getIngestMetadata().put("pipeline", pipelineId);

        verify(ingestService, Mockito.atLeast(2)).getPipeline(pipelineId);
        assertThat(resultList.size(), equalTo(5));

        assertNull(resultList.get(0).getConditionalWithResult());
        assertThat(resultList.get(0).getType(), equalTo("pipeline"));

        assertTrue(resultList.get(1).getIngestDocument().hasField(key1));
        assertFalse(resultList.get(1).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(1).getIngestDocument().hasField(key3));

        // failed processor
        assertNull(resultList.get(2).getIngestDocument());
        assertThat(resultList.get(2).getFailure().getMessage(), equalTo(exception.getMessage()));

        assertTrue(resultList.get(3).getIngestDocument().hasField(key1));
        assertTrue(resultList.get(3).getIngestDocument().hasField(key2));
        assertFalse(resultList.get(3).getIngestDocument().hasField(key3));

        assertIngestDocument(resultList.get(4).getIngestDocument(), expectedResult.getIngestDocument());
        assertThat(resultList.get(4).getFailure(), nullValue());
        assertThat(resultList.get(4).getProcessorTag(), nullValue());
    }

    public void testActualPipelineProcessorWithUnhandledFailure() throws Exception {
        String pipelineId = "pipeline1";
        IngestService ingestService = createIngestService();
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("name", pipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        String key1 = randomAlphaOfLength(10);
        IllegalStateException exception = new IllegalStateException("Not a pipeline cycle error");

        Pipeline pipeline = new Pipeline(
            pipelineId,
            null,
            null,
            null,
            new CompoundProcessor(
                new TestProcessor(ingestDocument -> ingestDocument.setFieldValue(key1, randomInt())),
                new TestProcessor(ingestDocument -> {
                    throw exception;
                })
            )
        );
        when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);

        PipelineProcessor pipelineProcessor = factory.create(Map.of(), null, null, pipelineConfig, null);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);

        trackingProcessor.execute(ingestDocument, (result, e) -> {});

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(
            actualProcessor.getType(),
            actualProcessor.getTag(),
            actualProcessor.getDescription(),
            ingestDocument,
            null
        );
        expectedResult.getIngestDocument().getIngestMetadata().put("pipeline", pipelineId);

        verify(ingestService, Mockito.atLeast(1)).getPipeline(pipelineId);

        assertThat(resultList.size(), equalTo(3));
        assertNull(resultList.get(0).getConditionalWithResult());
        assertThat(resultList.get(0).getType(), equalTo("pipeline"));
        assertTrue(resultList.get(1).getIngestDocument().hasField(key1));
        assertThat(resultList.get(2).getFailure(), equalTo(exception));
    }

    public void testActualPipelineProcessorWithCycle() throws Exception {
        String pipelineId1 = "pipeline1";
        String pipelineId2 = "pipeline2";
        IngestService ingestService = createIngestService();
        Map<String, Object> pipelineConfig0 = new HashMap<>();
        pipelineConfig0.put("name", pipelineId1);
        Map<String, Object> pipelineConfig1 = new HashMap<>();
        pipelineConfig1.put("name", pipelineId1);
        Map<String, Object> pipelineConfig2 = new HashMap<>();
        pipelineConfig2.put("name", pipelineId2);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        Pipeline pipeline1 = new Pipeline(
            pipelineId1,
            null,
            null,
            null,
            new CompoundProcessor(factory.create(Map.of(), null, null, pipelineConfig2, null))
        );

        Pipeline pipeline2 = new Pipeline(
            pipelineId2,
            null,
            null,
            null,
            new CompoundProcessor(factory.create(Map.of(), null, null, pipelineConfig1, null))
        );

        when(ingestService.getPipeline(pipelineId1)).thenReturn(pipeline1);
        when(ingestService.getPipeline(pipelineId2)).thenReturn(pipeline2);

        PipelineProcessor pipelineProcessor = factory.create(Map.of(), null, null, pipelineConfig0, null);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);

        Exception[] holder = new Exception[1];
        trackingProcessor.execute(ingestDocument, (result, e) -> holder[0] = e);
        IngestProcessorException exception = (IngestProcessorException) holder[0];
        assertThat(exception.getCause(), instanceOf(GraphStructureException.class));
        assertThat(exception.getMessage(), containsString("Cycle detected for pipeline: pipeline1"));
    }

    public void testActualPipelineProcessorNested() throws Exception {
        /*
         * This test creates a pipeline made up of many nested pipeline processors, ending in a processor that counts both how many times
         * it is called for a given document (by updating a field on that document) and how many times it is called overall.
         */
        IngestService ingestService = createIngestService();
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        int pipelineCount = randomIntBetween(2, IngestDocument.MAX_PIPELINES);
        for (int i = 0; i < pipelineCount - 1; i++) {
            String pipelineId = "pipeline" + i;
            String nextPipelineId = "pipeline" + (i + 1);
            Map<String, Object> nextPipelineConfig = new HashMap<>();
            nextPipelineConfig.put("name", nextPipelineId);
            Pipeline pipeline = new Pipeline(
                pipelineId,
                null,
                null,
                null,
                new CompoundProcessor(factory.create(Map.of(), null, null, nextPipelineConfig, null))
            );
            when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);
        }

        // The last pipeline calls the CountCallsProcessor rather than yet another pipeline processor:
        String lastPipelineId = "pipeline" + (pipelineCount - 1);
        CountCallsProcessor countCallsProcessor = new CountCallsProcessor();
        Pipeline lastPipeline = new Pipeline(lastPipelineId, null, null, null, new CompoundProcessor(countCallsProcessor));
        when(ingestService.getPipeline(lastPipelineId)).thenReturn(lastPipeline);

        String firstPipelineId = "pipeline0";
        Map<String, Object> firstPipelineConfig = new HashMap<>();
        firstPipelineConfig.put("name", firstPipelineId);
        PipelineProcessor pipelineProcessor = factory.create(Map.of(), null, null, firstPipelineConfig, null);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);

        IngestDocument[] holder = new IngestDocument[1];
        trackingProcessor.execute(ingestDocument, (result, e) -> holder[0] = result);
        IngestDocument document = holder[0];
        assertNotNull(document);
        // Make sure that the final processor was called exactly once on this document:
        assertThat(document.getFieldValue(countCallsProcessor.getCountFieldName(), Integer.class), equalTo(1));
        // But it was called exactly one other time during the pipeline cycle check:
        assertThat(countCallsProcessor.getTotalCount(), equalTo(2));
        assertThat(resultList.size(), equalTo(pipelineCount + 1)); // one result per pipeline, plus the "count_calls" processor
        for (int i = 0; i < resultList.size() - 1; i++) {
            SimulateProcessorResult result = resultList.get(i);
            assertThat(result.getType(), equalTo(pipelineProcessor.getType()));
        }
        assertThat(resultList.get(resultList.size() - 1).getType(), equalTo(countCallsProcessor.getType()));
    }

    public void testActualPipelineProcessorNestedTooManyPipelines() throws Exception {
        /*
         * This test creates a pipeline made up of many nested pipeline processors, ending in a processor that counts both how many times
         * it is called for a given document (by updating a field on that document) and how many times it is called overall.
         */
        IngestService ingestService = createIngestService();
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        int pipelineCount = randomIntBetween(IngestDocument.MAX_PIPELINES + 1, 500);
        for (int i = 0; i < pipelineCount - 1; i++) {
            String pipelineId = "pipeline" + i;
            String nextPipelineId = "pipeline" + (i + 1);
            Map<String, Object> nextPipelineConfig = new HashMap<>();
            nextPipelineConfig.put("name", nextPipelineId);
            Pipeline pipeline = new Pipeline(
                pipelineId,
                null,
                null,
                null,
                new CompoundProcessor(factory.create(Map.of(), null, null, nextPipelineConfig, null))
            );
            when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);
        }

        // The last pipeline calls the CountCallsProcessor rather than yet another pipeline processor:
        String lastPipelineId = "pipeline" + (pipelineCount - 1);
        CountCallsProcessor countCallsProcessor = new CountCallsProcessor();
        Pipeline lastPipeline = new Pipeline(lastPipelineId, null, null, null, new CompoundProcessor(countCallsProcessor));
        when(ingestService.getPipeline(lastPipelineId)).thenReturn(lastPipeline);

        String firstPipelineId = "pipeline0";
        Map<String, Object> firstPipelineConfig = new HashMap<>();
        firstPipelineConfig.put("name", firstPipelineId);
        PipelineProcessor pipelineProcessor = factory.create(Map.of(), null, null, firstPipelineConfig, null);
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);

        IngestDocument[] documentHolder = new IngestDocument[1];
        Exception[] exceptionHolder = new Exception[1];
        trackingProcessor.execute(ingestDocument, (result, e) -> {
            documentHolder[0] = result;
            exceptionHolder[0] = e;
        });
        IngestDocument document = documentHolder[0];
        Exception exception = exceptionHolder[0];
        assertNull(document);
        assertNotNull(exception);
        assertThat(exception.getMessage(), containsString("Too many nested pipelines"));
        // We expect that the last processor was never called:
        assertThat(countCallsProcessor.getTotalCount(), equalTo(0));
    }

    public void testActualPipelineProcessorRepeatedInvocation() throws Exception {
        String pipelineId = "pipeline1";
        IngestService ingestService = createIngestService();
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("name", pipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        String key1 = randomAlphaOfLength(10);
        PipelineProcessor pipelineProcessor = factory.create(Map.of(), null, null, pipelineConfig, null);
        Pipeline pipeline = new Pipeline(pipelineId, null, null, null, new CompoundProcessor(new TestProcessor(ingestDocument -> {
            ingestDocument.setFieldValue(key1, randomInt());
        })));
        when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);

        // calls the same pipeline twice
        CompoundProcessor actualProcessor = new CompoundProcessor(pipelineProcessor, pipelineProcessor);

        CompoundProcessor trackingProcessor = decorate(actualProcessor, null, resultList);

        trackingProcessor.execute(ingestDocument, (result, e) -> {});

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(
            actualProcessor.getType(),
            actualProcessor.getTag(),
            actualProcessor.getDescription(),
            ingestDocument,
            null
        );
        expectedResult.getIngestDocument().getIngestMetadata().put("pipeline", pipelineId);

        verify(ingestService, Mockito.atLeast(2)).getPipeline(pipelineId);
        assertThat(resultList.size(), equalTo(4));

        assertNull(resultList.get(0).getConditionalWithResult());
        assertThat(resultList.get(0).getType(), equalTo("pipeline"));

        assertThat(
            resultList.get(1).getIngestDocument().getFieldValue(key1, Integer.class),
            not(equalTo(expectedResult.getIngestDocument().getFieldValue(key1, Integer.class)))
        );
        assertThat(resultList.get(1).getFailure(), nullValue());
        assertThat(resultList.get(1).getProcessorTag(), nullValue());

        assertNull(resultList.get(2).getConditionalWithResult());
        assertThat(resultList.get(2).getType(), equalTo("pipeline"));

        assertIngestDocument(resultList.get(3).getIngestDocument(), expectedResult.getIngestDocument());
        assertThat(resultList.get(3).getFailure(), nullValue());
        assertThat(resultList.get(3).getProcessorTag(), nullValue());

        // each invocation updates key1 with a random int
        assertNotEquals(
            resultList.get(1).getIngestDocument().getSourceAndMetadata().get(key1),
            resultList.get(3).getIngestDocument().getSourceAndMetadata().get(key1)
        );
    }

    /*
     * This test processor keeps track of how many times it has been called. It also creates/updates the "count" field on documents with
     * the number of times this processor has been called for that document.
     */
    private static final class CountCallsProcessor extends AbstractProcessor {
        private int totalCount = 0;

        private CountCallsProcessor() {
            super("count_calls", "counts calls");
        }

        @Override
        public String getType() {
            return "count_calls";
        }

        @Override
        public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
            boolean hasCount = ingestDocument.hasField(getCountFieldName());
            Integer count;
            if (hasCount) {
                count = ingestDocument.getFieldValue(getCountFieldName(), Integer.class);
            } else {
                count = 0;
            }
            ingestDocument.setFieldValue(getCountFieldName(), (count + 1));
            totalCount++;
            return ingestDocument;
        }

        public String getCountFieldName() {
            return "count";
        }

        public int getTotalCount() {
            return totalCount;
        }
    }
}
