/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GetPipelineResponseTests extends ESTestCase {

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
        GetPipelineResponse parsedResponse;
        try (
            XContentParser parser = builder.generator()
                .contentType()
                .xContent()
                .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput())
        ) {
            parsedResponse = doParseInstance(parser);
        }
        List<PipelineConfiguration> actualPipelines = response.pipelines();
        List<PipelineConfiguration> parsedPipelines = parsedResponse.pipelines();
        assertEquals(actualPipelines.size(), parsedPipelines.size());
        for (PipelineConfiguration pipeline : parsedPipelines) {
            assertTrue(pipelinesMap.containsKey(pipeline.getId()));
            assertEquals(pipelinesMap.get(pipeline.getId()).getConfig(), pipeline.getConfig());
        }
    }

    protected GetPipelineResponse doParseInstance(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        List<PipelineConfiguration> pipelines = new ArrayList<>();
        while (parser.nextToken().equals(XContentParser.Token.FIELD_NAME)) {
            String pipelineId = parser.currentName();
            parser.nextToken();
            try (XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent())) {
                contentBuilder.generator().copyCurrentStructure(parser);
                PipelineConfiguration pipeline = new PipelineConfiguration(
                    pipelineId,
                    BytesReference.bytes(contentBuilder),
                    contentBuilder.contentType()
                );
                pipelines.add(pipeline);
            }
        }
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser);
        return new GetPipelineResponse(pipelines);
    }

}
