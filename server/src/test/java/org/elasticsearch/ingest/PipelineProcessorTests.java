/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.ingest;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipelineProcessorTests extends ESTestCase {

    public void testExecutesPipeline() throws Exception {
        String pipelineId = "pipeline";
        IngestService ingestService = createIngestService();
        PlainActionFuture<IngestDocument> invoked = new PlainActionFuture<>();
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Pipeline pipeline = new Pipeline(pipelineId, null, null, null, new CompoundProcessor(new Processor() {
            @Override
            public IngestDocument execute(final IngestDocument ingestDocument) {
                invoked.onResponse(ingestDocument);
                return ingestDocument;
            }

            @Override
            public String getType() {
                return null;
            }

            @Override
            public String getTag() {
                return null;
            }

            @Override
            public String getDescription() {
                return null;
            }
        }));
        when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Map<String, Object> config = new HashMap<>();
        config.put("name", pipelineId);
        factory.create(Map.of(), null, null, config, null).execute(testIngestDocument, (result, e) -> {});
        assertIngestDocument(testIngestDocument, safeGet(invoked));
    }

    public void testThrowsOnMissingPipeline() throws Exception {
        IngestService ingestService = createIngestService();
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Map<String, Object> config = new HashMap<>();
        config.put("name", "missingPipelineId");
        IllegalStateException[] e = new IllegalStateException[1];
        factory.create(Map.of(), null, null, config, null).execute(testIngestDocument, (result, e1) -> e[0] = (IllegalStateException) e1);
        assertEquals("Pipeline processor configured for non-existent pipeline [missingPipelineId]", e[0].getMessage());
    }

    public void testIgnoreMissingPipeline() throws Exception {
        var ingestService = createIngestService();
        var testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        var factory = new PipelineProcessor.Factory(ingestService);
        var config = new HashMap<String, Object>();
        config.put("name", "missingPipelineId");
        config.put("ignore_missing_pipeline", true);

        var r = new IngestDocument[1];
        var e = new Exception[1];
        var processor = factory.create(Map.of(), null, null, config, null);
        processor.execute(testIngestDocument, (result, e1) -> {
            r[0] = result;
            e[0] = e1;
        });
        assertNull(e[0]);
        assertSame(testIngestDocument, r[0]);
    }

    public void testThrowsOnRecursivePipelineInvocations() throws Exception {
        String innerPipelineId = "inner";
        String outerPipelineId = "outer";
        IngestService ingestService = createIngestService();
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Map<String, Object> outerConfig = new HashMap<>();
        outerConfig.put("name", innerPipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Pipeline outer = new Pipeline(
            outerPipelineId,
            null,
            null,
            null,
            new CompoundProcessor(factory.create(Map.of(), null, null, outerConfig, null))
        );
        Map<String, Object> innerConfig = new HashMap<>();
        innerConfig.put("name", outerPipelineId);
        Pipeline inner = new Pipeline(
            innerPipelineId,
            null,
            null,
            null,
            new CompoundProcessor(factory.create(Map.of(), null, null, innerConfig, null))
        );
        when(ingestService.getPipeline(outerPipelineId)).thenReturn(outer);
        when(ingestService.getPipeline(innerPipelineId)).thenReturn(inner);
        outerConfig.put("name", innerPipelineId);
        ElasticsearchException[] e = new ElasticsearchException[1];
        factory.create(Map.of(), null, null, outerConfig, null)
            .execute(testIngestDocument, (result, e1) -> e[0] = (ElasticsearchException) e1);
        assertEquals("Cycle detected for pipeline: inner", e[0].getRootCause().getMessage());
    }

    public void testAllowsRepeatedPipelineInvocations() throws Exception {
        String innerPipelineId = "inner";
        IngestService ingestService = createIngestService();
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Map<String, Object> outerConfig = new HashMap<>();
        outerConfig.put("name", innerPipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Pipeline inner = new Pipeline(innerPipelineId, null, null, null, new CompoundProcessor());
        when(ingestService.getPipeline(innerPipelineId)).thenReturn(inner);
        Processor outerProc = factory.create(Map.of(), null, null, outerConfig, null);
        outerProc.execute(testIngestDocument, (result, e) -> {});
        outerProc.execute(testIngestDocument, (result, e) -> {});
    }

    public void testPipelineProcessorWithPipelineChain() throws Exception {
        String pipeline1Id = "pipeline1";
        String pipeline2Id = "pipeline2";
        String pipeline3Id = "pipeline3";
        IngestService ingestService = createIngestService();
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        Map<String, Object> pipeline1ProcessorConfig = new HashMap<>();
        pipeline1ProcessorConfig.put("name", pipeline2Id);
        PipelineProcessor pipeline1Processor = factory.create(Map.of(), null, null, pipeline1ProcessorConfig, null);

        Map<String, Object> pipeline2ProcessorConfig = new HashMap<>();
        pipeline2ProcessorConfig.put("name", pipeline3Id);
        PipelineProcessor pipeline2Processor = factory.create(Map.of(), null, null, pipeline2ProcessorConfig, null);

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        Pipeline pipeline1 = new Pipeline(
            pipeline1Id,
            null,
            null,
            null,
            new CompoundProcessor(pipeline1Processor),
            relativeTimeProvider,
            null
        );

        String key1 = randomAlphaOfLength(10);
        relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(3));
        Pipeline pipeline2 = new Pipeline(
            pipeline2Id,
            null,
            null,
            null,
            new CompoundProcessor(true, List.of(new TestProcessor(ingestDocument -> {
                ingestDocument.setFieldValue(key1, randomInt());
            }), pipeline2Processor), List.of()),
            relativeTimeProvider,
            null
        );
        relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(2));
        Pipeline pipeline3 = new Pipeline(pipeline3Id, null, null, null, new CompoundProcessor(new TestProcessor(ingestDocument -> {
            throw new RuntimeException("error");
        })), relativeTimeProvider, null);
        when(ingestService.getPipeline(pipeline1Id)).thenReturn(pipeline1);
        when(ingestService.getPipeline(pipeline2Id)).thenReturn(pipeline2);
        when(ingestService.getPipeline(pipeline3Id)).thenReturn(pipeline3);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        // start the chain
        ingestDocument.executePipeline(pipeline1, (result, e) -> {});
        assertNotNull(ingestDocument.getSourceAndMetadata().get(key1));

        // check the stats
        IngestStats.Stats pipeline1Stats = pipeline1.getMetrics().createStats();
        IngestStats.Stats pipeline2Stats = pipeline2.getMetrics().createStats();
        IngestStats.Stats pipeline3Stats = pipeline3.getMetrics().createStats();

        // current
        assertThat(pipeline1Stats.ingestCurrent(), equalTo(0L));
        assertThat(pipeline2Stats.ingestCurrent(), equalTo(0L));
        assertThat(pipeline3Stats.ingestCurrent(), equalTo(0L));

        // count
        assertThat(pipeline1Stats.ingestCount(), equalTo(1L));
        assertThat(pipeline2Stats.ingestCount(), equalTo(1L));
        assertThat(pipeline3Stats.ingestCount(), equalTo(1L));

        // time
        assertThat(pipeline1Stats.ingestTimeInMillis(), equalTo(0L));
        assertThat(pipeline2Stats.ingestTimeInMillis(), equalTo(3L));
        assertThat(pipeline3Stats.ingestTimeInMillis(), equalTo(2L));

        // failure
        assertThat(pipeline1Stats.ingestFailedCount(), equalTo(0L));
        assertThat(pipeline2Stats.ingestFailedCount(), equalTo(0L));
        assertThat(pipeline3Stats.ingestFailedCount(), equalTo(1L));
    }

    public void testIngestPipelineMetadata() {
        IngestService ingestService = createIngestService();

        final int numPipelines = 16;
        Pipeline firstPipeline = null;
        for (int i = 0; i < numPipelines; i++) {
            String pipelineId = Integer.toString(i);
            List<Processor> processors = new ArrayList<>();
            processors.add(new AbstractProcessor(null, null) {
                @Override
                public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                    ingestDocument.appendFieldValue("pipelines", ingestDocument.getIngestMetadata().get("pipeline"));
                    return ingestDocument;
                }

                @Override
                public String getType() {
                    return null;
                }

            });
            if (i < (numPipelines - 1)) {
                TemplateScript.Factory pipelineName = new TestTemplateService.MockTemplateScript.Factory(Integer.toString(i + 1));
                processors.add(new PipelineProcessor(null, null, pipelineName, false, ingestService));
            }

            Pipeline pipeline = new Pipeline(pipelineId, null, null, null, new CompoundProcessor(false, processors, List.of()));
            when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);
            if (firstPipeline == null) {
                firstPipeline = pipeline;
            }
        }

        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument[] docHolder = new IngestDocument[1];
        Exception[] errorHolder = new Exception[1];
        testIngestDocument.executePipeline(firstPipeline, (doc, e) -> {
            docHolder[0] = doc;
            errorHolder[0] = e;
        });
        assertThat(docHolder[0], notNullValue());
        assertThat(errorHolder[0], nullValue());

        IngestDocument ingestDocument = docHolder[0];
        List<?> pipelines = ingestDocument.getFieldValue("pipelines", List.class);
        assertThat(pipelines.size(), equalTo(numPipelines));
        for (int i = 0; i < numPipelines; i++) {
            assertThat(pipelines.get(i), equalTo(Integer.toString(i)));
        }
    }

    static IngestService createIngestService() {
        IngestService ingestService = mock(IngestService.class);
        ScriptService scriptService = mock(ScriptService.class);
        when(ingestService.getScriptService()).thenReturn(scriptService);
        return ingestService;
    }
}
