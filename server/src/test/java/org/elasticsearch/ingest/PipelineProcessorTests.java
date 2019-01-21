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
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipelineProcessorTests extends ESTestCase {

    public void testExecutesPipeline() throws Exception {
        String pipelineId = "pipeline";
        IngestService ingestService = mock(IngestService.class);
        CompletableFuture<IngestDocument> invoked = new CompletableFuture<>();
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Pipeline pipeline = new Pipeline(
            pipelineId, null, null,
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
            })
        );
        when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Map<String, Object> config = new HashMap<>();
        config.put("name", pipelineId);
        factory.create(Collections.emptyMap(), null, config).execute(testIngestDocument);
        assertEquals(testIngestDocument, invoked.get());
    }

    public void testThrowsOnMissingPipeline() throws Exception {
        IngestService ingestService = mock(IngestService.class);
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Map<String, Object> config = new HashMap<>();
        config.put("name", "missingPipelineId");
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> factory.create(Collections.emptyMap(), null, config).execute(testIngestDocument)
        );
        assertEquals(
            "Pipeline processor configured for non-existent pipeline [missingPipelineId]", e.getMessage()
        );
    }

    public void testThrowsOnRecursivePipelineInvocations() throws Exception {
        String innerPipelineId = "inner";
        String outerPipelineId = "outer";
        IngestService ingestService = mock(IngestService.class);
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Map<String, Object> outerConfig = new HashMap<>();
        outerConfig.put("name", innerPipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Pipeline outer = new Pipeline(
            outerPipelineId, null, null,
            new CompoundProcessor(factory.create(Collections.emptyMap(), null, outerConfig))
        );
        Map<String, Object> innerConfig = new HashMap<>();
        innerConfig.put("name", outerPipelineId);
        Pipeline inner = new Pipeline(
            innerPipelineId, null, null,
            new CompoundProcessor(factory.create(Collections.emptyMap(), null, innerConfig))
        );
        when(ingestService.getPipeline(outerPipelineId)).thenReturn(outer);
        when(ingestService.getPipeline(innerPipelineId)).thenReturn(inner);
        outerConfig.put("name", innerPipelineId);
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> factory.create(Collections.emptyMap(), null, outerConfig).execute(testIngestDocument)
        );
        assertEquals(
            "Cycle detected for pipeline: inner", e.getRootCause().getMessage()
        );
    }

    public void testAllowsRepeatedPipelineInvocations() throws Exception {
        String innerPipelineId = "inner";
        IngestService ingestService = mock(IngestService.class);
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Map<String, Object> outerConfig = new HashMap<>();
        outerConfig.put("name", innerPipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Pipeline inner = new Pipeline(
            innerPipelineId, null, null, new CompoundProcessor()
        );
        when(ingestService.getPipeline(innerPipelineId)).thenReturn(inner);
        Processor outerProc = factory.create(Collections.emptyMap(), null, outerConfig);
        outerProc.execute(testIngestDocument);
        outerProc.execute(testIngestDocument);
    }

    public void testPipelineProcessorWithPipelineChain() throws Exception {
        String pipeline1Id = "pipeline1";
        String pipeline2Id = "pipeline2";
        String pipeline3Id = "pipeline3";
        IngestService ingestService = mock(IngestService.class);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        Map<String, Object> pipeline1ProcessorConfig = new HashMap<>();
        pipeline1ProcessorConfig.put("name", pipeline2Id);
        PipelineProcessor pipeline1Processor = factory.create(Collections.emptyMap(), null, pipeline1ProcessorConfig);

        Map<String, Object> pipeline2ProcessorConfig = new HashMap<>();
        pipeline2ProcessorConfig.put("name", pipeline3Id);
        PipelineProcessor pipeline2Processor = factory.create(Collections.emptyMap(), null, pipeline2ProcessorConfig);

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        Pipeline pipeline1 = new Pipeline(
            pipeline1Id, null, null, new CompoundProcessor(pipeline1Processor), relativeTimeProvider
        );

        String key1 = randomAlphaOfLength(10);
        relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(3));
        Pipeline pipeline2 = new Pipeline(
            pipeline2Id, null, null, new CompoundProcessor(true,
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
            pipeline3Id, null, null, new CompoundProcessor(
            new TestProcessor(ingestDocument -> {
                throw new RuntimeException("error");
            })), relativeTimeProvider
        );
        when(ingestService.getPipeline(pipeline1Id)).thenReturn(pipeline1);
        when(ingestService.getPipeline(pipeline2Id)).thenReturn(pipeline2);
        when(ingestService.getPipeline(pipeline3Id)).thenReturn(pipeline3);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        //start the chain
        ingestDocument.executePipeline(pipeline1);
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
}
