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
package org.elasticsearch.ingest.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

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
                public void execute(final IngestDocument ingestDocument) throws Exception {
                    invoked.complete(ingestDocument);
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
        config.put("pipeline", pipelineId);
        factory.create(Collections.emptyMap(), null, config).execute(testIngestDocument);
        assertEquals(testIngestDocument, invoked.get());
    }
}
