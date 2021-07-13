/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GetPipelineResponse extends ActionResponse implements StatusToXContentObject {

    private List<PipelineConfiguration> pipelines;
    private final boolean summary;

    public GetPipelineResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        pipelines = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            pipelines.add(PipelineConfiguration.readFrom(in));
        }
        summary = in.readBoolean();
    }

    public GetPipelineResponse(List<PipelineConfiguration> pipelines, boolean summary) {
        this.pipelines = pipelines;
        this.summary = summary;
    }

    public GetPipelineResponse(List<PipelineConfiguration> pipelines) {
        this(pipelines, false);
    }

    /**
     * Get the list of pipelines that were a part of this response.
     * The pipeline id can be obtained using getId on the PipelineConfiguration object.
     * @return A list of {@link PipelineConfiguration} objects.
     */
    public List<PipelineConfiguration> pipelines() {
        return Collections.unmodifiableList(pipelines);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(pipelines.size());
        for (PipelineConfiguration pipeline : pipelines) {
            pipeline.writeTo(out);
        }
        out.writeBoolean(summary);
    }

    public boolean isFound() {
        return pipelines.isEmpty() == false;
    }

    public boolean isSummary() {
        return summary;
    }

    @Override
    public RestStatus status() {
        return isFound() ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (PipelineConfiguration pipeline : pipelines) {
            builder.field(pipeline.getId(), summary ? Map.of() : pipeline.getConfigAsMap());
        }
        builder.endObject();
        return builder;
    }

    /**
     *
     * @param parser the parser for the XContent that contains the serialized GetPipelineResponse.
     * @return an instance of GetPipelineResponse read from the parser
     * @throws IOException If the parsing fails
     */
    public static GetPipelineResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        List<PipelineConfiguration> pipelines = new ArrayList<>();
        while(parser.nextToken().equals(Token.FIELD_NAME)) {
            String pipelineId = parser.currentName();
            parser.nextToken();
            try (XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent())) {
                contentBuilder.generator().copyCurrentStructure(parser);
                PipelineConfiguration pipeline =
                    new PipelineConfiguration(pipelineId, BytesReference.bytes(contentBuilder), contentBuilder.contentType());
                pipelines.add(pipeline);
            }
        }
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser);
        return new GetPipelineResponse(pipelines);
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        } else if (other instanceof GetPipelineResponse){
            GetPipelineResponse otherResponse = (GetPipelineResponse)other;
            if (pipelines == null) {
                return otherResponse.pipelines == null;
            } else {
                // We need a map here because order does not matter for equality
                Map<String, PipelineConfiguration> otherPipelineMap = new HashMap<>();
                for (PipelineConfiguration pipeline: otherResponse.pipelines) {
                    otherPipelineMap.put(pipeline.getId(), pipeline);
                }
                for (PipelineConfiguration pipeline: pipelines) {
                    PipelineConfiguration otherPipeline = otherPipelineMap.get(pipeline.getId());
                    if (pipeline.equals(otherPipeline) == false) {
                        return false;
                    }
                }
                return true;
            }
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (PipelineConfiguration pipeline: pipelines) {
            // We only take the sum here to ensure that the order does not matter.
            result += (pipeline == null ? 0 : pipeline.hashCode());
        }
        return result;
    }

}
