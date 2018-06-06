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

package org.elasticsearch.client;

import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.ingest.SimulateDocumentResult;
import org.elasticsearch.action.ingest.SimulateDocumentVerboseResult;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.ingest.WritePipelineResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.ingest.RandomDocumentPicks;

import java.io.IOException;
import java.util.Map;

public class IngestClientIT extends ESRestHighLevelClientTestCase {

    public void testPutPipeline() throws IOException {
        String id = "some_pipeline_id";
        XContentBuilder pipelineBuilder = buildRandomXContentPipeline();
        PutPipelineRequest request = new PutPipelineRequest(
            id,
            BytesReference.bytes(pipelineBuilder),
            pipelineBuilder.contentType());

        WritePipelineResponse putPipelineResponse =
            execute(request, highLevelClient().ingest()::putPipeline, highLevelClient().ingest()::putPipelineAsync);
        assertTrue(putPipelineResponse.isAcknowledged());
    }

    public void testGetPipeline() throws IOException {
        String id = "some_pipeline_id";
        XContentBuilder pipelineBuilder = buildRandomXContentPipeline();
        {
            PutPipelineRequest request = new PutPipelineRequest(
                id,
                BytesReference.bytes(pipelineBuilder),
                pipelineBuilder.contentType()
            );
            createPipeline(request);
        }

        GetPipelineRequest request = new GetPipelineRequest(id);

        GetPipelineResponse response =
            execute(request, highLevelClient().ingest()::getPipeline, highLevelClient().ingest()::getPipelineAsync);
        assertTrue(response.isFound());
        assertEquals(response.pipelines().get(0).getId(), id);
        PipelineConfiguration expectedConfig =
            new PipelineConfiguration(id, BytesReference.bytes(pipelineBuilder), pipelineBuilder.contentType());
        assertEquals(expectedConfig.getConfigAsMap(), response.pipelines().get(0).getConfigAsMap());
    }

    public void testDeletePipeline() throws IOException {
        String id = "some_pipeline_id";
        {
            createPipeline(id);
        }

        DeletePipelineRequest request = new DeletePipelineRequest(id);

        WritePipelineResponse response =
            execute(request, highLevelClient().ingest()::deletePipeline, highLevelClient().ingest()::deletePipelineAsync);
        assertTrue(response.isAcknowledged());
    }

    public void testSimulatePipeline() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        int numDocs = randomIntBetween(1, 10);
        boolean isVerbose = randomBoolean();
        builder.startObject();
        {
            builder.field("pipeline");
            buildRandomXContentPipeline(builder);
            builder.startArray("docs");
            {
                for (int i = 0; i < numDocs; i++) {
                    builder.startObject();
                    IngestDocument document = RandomDocumentPicks.randomIngestDocument(random());
                    Map<IngestDocument.MetaData, Object> metadataMap = document.extractMetadata();
                    for (Map.Entry<IngestDocument.MetaData, Object> metadata : metadataMap.entrySet()) {
                        if (metadata.getValue() != null) {
                            if (metadata.getKey().equals(IngestDocument.MetaData.VERSION)) {
                                builder.field(metadata.getKey().getFieldName(), (long)metadata.getValue());
                            } else {
                                builder.field(metadata.getKey().getFieldName(), metadata.getValue().toString());
                            }
                        }
                    }
                    document.setFieldValue("rank", Integer.toString(randomInt()));
                    builder.field("_source", document.getSourceAndMetadata());
                    builder.endObject();
                }
            }
            builder.endArray();
        }
        builder.endObject();

        SimulatePipelineRequest request = new SimulatePipelineRequest(
            BytesReference.bytes(builder),
            builder.contentType()
        );
        request.setVerbose(isVerbose);

        SimulatePipelineResponse simulatePipelineResponse =
            execute(request, highLevelClient().ingest()::simulatePipeline, highLevelClient().ingest()::simulatePipelineAsync);

        for (SimulateDocumentResult result: simulatePipelineResponse.getResults()) {
            if (isVerbose) {
                assertTrue(result instanceof SimulateDocumentVerboseResult);
                SimulateDocumentVerboseResult verboseResult = (SimulateDocumentVerboseResult)result;
                assertTrue(verboseResult.getProcessorResults().size() > 0);
                assertEquals(
                    verboseResult.getProcessorResults().get(0).getIngestDocument()
                        .getFieldValue("foo", String.class),
                    "bar"
                );
            } else {
                assertTrue(result instanceof SimulateDocumentBaseResult);
                SimulateDocumentBaseResult baseResult = (SimulateDocumentBaseResult)result;
                assertNotNull(baseResult.getIngestDocument());
                assertEquals(
                    baseResult.getIngestDocument().getFieldValue("foo", String.class),
                    "bar"
                );
                assertNotNull(
                    baseResult.getIngestDocument().getFieldValue("rank", Integer.class)
                );
            }
        }
    }
}
