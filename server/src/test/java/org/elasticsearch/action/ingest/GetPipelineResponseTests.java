/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetPipelineResponseTests extends AbstractXContentSerializingTestCase<GetPipelineResponse> {

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
        return new PipelineConfiguration(pipelineId, BytesReference.bytes(builder), builder.contentType());
    }

    private Map<String, PipelineConfiguration> createPipelineConfigMap() throws IOException {
        int numPipelines = randomInt(5);
        Map<String, PipelineConfiguration> pipelinesMap = new HashMap<>();
        for (int i = 0; i < numPipelines; i++) {
            String pipelineId = "pipeline_" + i;
            pipelinesMap.put(pipelineId, createRandomPipeline(pipelineId));
        }
        return pipelinesMap;
    }

    public void testXContentDeserialization() throws IOException {
        Map<String, PipelineConfiguration> pipelinesMap = createPipelineConfigMap();
        GetPipelineResponse response = new GetPipelineResponse(new ArrayList<>(pipelinesMap.values()));
        XContentBuilder builder = response.toXContent(getRandomXContentBuilder(), ToXContent.EMPTY_PARAMS);
        XContentParser parser = builder.generator()
            .contentType()
            .xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        GetPipelineResponse parsedResponse = GetPipelineResponse.fromXContent(parser);
        List<PipelineConfiguration> actualPipelines = response.pipelines();
        List<PipelineConfiguration> parsedPipelines = parsedResponse.pipelines();
        assertEquals(actualPipelines.size(), parsedPipelines.size());
        for (PipelineConfiguration pipeline : parsedPipelines) {
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
    protected GetPipelineResponse mutateInstance(GetPipelineResponse response) throws IOException {
        return new GetPipelineResponse(
            CollectionUtils.appendToCopy(response.pipelines(), createRandomPipeline("pipeline_" + response.pipelines().size() + 1))
        );
    }
}
