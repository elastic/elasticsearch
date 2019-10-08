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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetPipelineResponseTests extends AbstractSerializingTestCase<GetPipelineResponse> {

    private XContentBuilder getRandomXContentBuilder() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        return XContentBuilder.builder(xContentType.xContent());
    }

    private PipelineConfiguration createRandomPipeline(String pipelineId) throws IOException {
        String field = "field_" + randomInt();
        String value = "value_" + randomInt();
        XContentBuilder builder = getRandomXContentBuilder();
        builder.startObject();
        // We only use a single SetProcessor here in each pipeline to test.
        // Since the contents are returned as a configMap anyway this does not matter for fromXContent
        builder.startObject("set");
        builder.field("field", field);
        builder.field("value", value);
        builder.endObject();
        builder.endObject();
        return
            new PipelineConfiguration(
                pipelineId, BytesReference.bytes(builder), builder.contentType()
            );
    }

    private Map<String, PipelineConfiguration> createPipelineConfigMap() throws IOException {
        int numPipelines = randomInt(5);
        Map<String, PipelineConfiguration> pipelinesMap = new HashMap<>();
        for (int i=0; i<numPipelines; i++) {
            String pipelineId = "pipeline_" + i;
            pipelinesMap.put(pipelineId, createRandomPipeline(pipelineId));
        }
        return pipelinesMap;
    }

    public void testXContentDeserialization() throws IOException {
        Map<String, PipelineConfiguration> pipelinesMap = createPipelineConfigMap();
        GetPipelineResponse response = new GetPipelineResponse(new ArrayList<>(pipelinesMap.values()));
        XContentBuilder builder = response.toXContent(getRandomXContentBuilder(), ToXContent.EMPTY_PARAMS);
        XContentParser parser =
            builder
                .generator()
                .contentType()
                .xContent()
                .createParser(
                    xContentRegistry(),
                    LoggingDeprecationHandler.INSTANCE,
                    BytesReference.bytes(builder).streamInput()
                );
        GetPipelineResponse parsedResponse = GetPipelineResponse.fromXContent(parser);
        List<PipelineConfiguration> actualPipelines = response.pipelines();
        List<PipelineConfiguration> parsedPipelines = parsedResponse.pipelines();
        assertEquals(actualPipelines.size(), parsedPipelines.size());
        for (PipelineConfiguration pipeline: parsedPipelines) {
            assertTrue(pipelinesMap.containsKey(pipeline.getId()));
            assertEquals(pipelinesMap.get(pipeline.getId()).getConfigAsMap(), pipeline.getConfigAsMap());
        }
    }

    @Override
    protected GetPipelineResponse doParseInstance(XContentParser parser) throws IOException {
        return GetPipelineResponse.fromXContent(parser);
    }

    @Override
    protected GetPipelineResponse createTestInstance() {
        try {
            return new GetPipelineResponse(new ArrayList<>(createPipelineConfigMap().values()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected Writeable.Reader<GetPipelineResponse> instanceReader() {
        return GetPipelineResponse::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected GetPipelineResponse mutateInstance(GetPipelineResponse response) {
        try {
            List<PipelineConfiguration> clonePipelines = new ArrayList<>(response.pipelines());
            clonePipelines.add(createRandomPipeline("pipeline_" + clonePipelines.size() + 1));
            return new GetPipelineResponse(clonePipelines);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
