/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipelineProcessorTests extends ESTestCase {

    public void testExecutesPipeline() throws Exception {
        String pipelineId = "pipeline";
        IngestService ingestService = createIngestService();
        CompletableFuture<IngestDocument> invoked = new CompletableFuture<>();
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Pipeline pipeline = new Pipeline(
            pipelineId, null, null, null,
            new CompoundProcessor(new Processor() {
                @Override
                public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                    invoked.complete(ingestDocument);
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
            })
        );
        when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Map<String, Object> config = new HashMap<>();
        config.put("name", pipelineId);
        factory.create(Collections.emptyMap(), null, null, config).execute(testIngestDocument, (result, e) -> {});
        assertEquals(testIngestDocument, invoked.get());
    }

    public void testThrowsOnMissingPipeline() throws Exception {
        IngestService ingestService = createIngestService();
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Map<String, Object> config = new HashMap<>();
        config.put("name", "missingPipelineId");
        IllegalStateException[] e = new IllegalStateException[1];
        factory.create(Collections.emptyMap(), null, null, config)
            .execute(testIngestDocument, (result, e1) -> e[0] = (IllegalStateException) e1);
        assertEquals(
            "Pipeline processor configured for non-existent pipeline [missingPipelineId]", e[0].getMessage()
        );
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
            outerPipelineId, null, null, null,
            new CompoundProcessor(factory.create(Collections.emptyMap(), null, null, outerConfig))
        );
        Map<String, Object> innerConfig = new HashMap<>();
        innerConfig.put("name", outerPipelineId);
        Pipeline inner = new Pipeline(
            innerPipelineId, null, null, null,
            new CompoundProcessor(factory.create(Collections.emptyMap(), null, null, innerConfig))
        );
        when(ingestService.getPipeline(outerPipelineId)).thenReturn(outer);
        when(ingestService.getPipeline(innerPipelineId)).thenReturn(inner);
        outerConfig.put("name", innerPipelineId);
        ElasticsearchException[] e = new ElasticsearchException[1];
        factory.create(Collections.emptyMap(), null, null, outerConfig)
            .execute(testIngestDocument, (result, e1) -> e[0] = (ElasticsearchException) e1);
        assertEquals(
            "Cycle detected for pipeline: inner", e[0].getRootCause().getMessage()
        );
    }

    public void testAllowsRepeatedPipelineInvocations() throws Exception {
        String innerPipelineId = "inner";
        IngestService ingestService = createIngestService();
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Map<String, Object> outerConfig = new HashMap<>();
        outerConfig.put("name", innerPipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Pipeline inner = new Pipeline(
            innerPipelineId, null, null, null, new CompoundProcessor()
        );
        when(ingestService.getPipeline(innerPipelineId)).thenReturn(inner);
        Processor outerProc = factory.create(Collections.emptyMap(), null, null, outerConfig);
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
        PipelineProcessor pipeline1Processor = factory.create(Collections.emptyMap(), null, null, pipeline1ProcessorConfig);

        Map<String, Object> pipeline2ProcessorConfig = new HashMap<>();
        pipeline2ProcessorConfig.put("name", pipeline3Id);
        PipelineProcessor pipeline2Processor = factory.create(Collections.emptyMap(), null, null, pipeline2ProcessorConfig);

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        Pipeline pipeline1 = new Pipeline(
            pipeline1Id, null, null, null, new CompoundProcessor(pipeline1Processor), relativeTimeProvider
        );

        String key1 = randomAlphaOfLength(10);
        relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(3));
        Pipeline pipeline2 = new Pipeline(
            pipeline2Id, null, null, null, new CompoundProcessor(true,
            Arrays.asList(
                new TestProcessor(ingestDocument -> {
                    ingestDocument.setFieldValue(key1, randomInt());
                }),
                pipeline2Processor),
            Collections.emptyList()),
            relativeTimeProvider
        );
        relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(2));
        Pipeline pipeline3 = new Pipeline(
            pipeline3Id, null, null, null, new CompoundProcessor(
            new TestProcessor(ingestDocument -> {
                throw new RuntimeException("error");
            })), relativeTimeProvider
        );
        when(ingestService.getPipeline(pipeline1Id)).thenReturn(pipeline1);
        when(ingestService.getPipeline(pipeline2Id)).thenReturn(pipeline2);
        when(ingestService.getPipeline(pipeline3Id)).thenReturn(pipeline3);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        //start the chain
        ingestDocument.executePipeline(pipeline1, (result, e) -> {});
        assertNotNull(ingestDocument.getSourceAndMetadata().get(key1));

        //check the stats
        IngestStats.Stats pipeline1Stats = pipeline1.getMetrics().createStats();
        IngestStats.Stats pipeline2Stats = pipeline2.getMetrics().createStats();
        IngestStats.Stats pipeline3Stats = pipeline3.getMetrics().createStats();

        //current
        assertThat(pipeline1Stats.getIngestCurrent(), equalTo(0L));
        assertThat(pipeline2Stats.getIngestCurrent(), equalTo(0L));
        assertThat(pipeline3Stats.getIngestCurrent(), equalTo(0L));

        //count
        assertThat(pipeline1Stats.getIngestCount(), equalTo(1L));
        assertThat(pipeline2Stats.getIngestCount(), equalTo(1L));
        assertThat(pipeline3Stats.getIngestCount(), equalTo(1L));

        //time
        assertThat(pipeline1Stats.getIngestTimeInMillis(), equalTo(0L));
        assertThat(pipeline2Stats.getIngestTimeInMillis(), equalTo(3L));
        assertThat(pipeline3Stats.getIngestTimeInMillis(), equalTo(2L));

        //failure
        assertThat(pipeline1Stats.getIngestFailedCount(), equalTo(0L));
        assertThat(pipeline2Stats.getIngestFailedCount(), equalTo(0L));
        assertThat(pipeline3Stats.getIngestFailedCount(), equalTo(1L));
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
                processors.add(new PipelineProcessor(null, null, pipelineName, ingestService));
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
