/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class SimulatePipelineResponse extends ActionResponse implements ToXContentObject {
    private String pipelineId;
    private boolean verbose;
    private List<SimulateDocumentResult> results;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SimulatePipelineResponse, Void> PARSER = new ConstructingObjectParser<>(
        "simulate_pipeline_response",
        true,
        a -> {
            List<SimulateDocumentResult> results = (List<SimulateDocumentResult>) a[0];
            boolean verbose = false;
            if (results.size() > 0) {
                if (results.get(0) instanceof SimulateDocumentVerboseResult) {
                    verbose = true;
                }
            }
            return new SimulatePipelineResponse(null, verbose, results);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), (parser, context) -> {
            Token token = parser.currentToken();
            ensureExpectedToken(Token.START_OBJECT, token, parser);
            SimulateDocumentResult result = null;
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                ensureExpectedToken(Token.FIELD_NAME, token, parser);
                String fieldName = parser.currentName();
                token = parser.nextToken();
                if (token == Token.START_ARRAY) {
                    if (fieldName.equals(SimulateDocumentVerboseResult.PROCESSOR_RESULT_FIELD)) {
                        List<SimulateProcessorResult> results = new ArrayList<>();
                        while ((token = parser.nextToken()) == Token.START_OBJECT) {
                            results.add(SimulateProcessorResult.fromXContent(parser));
                        }
                        ensureExpectedToken(Token.END_ARRAY, token, parser);
                        result = new SimulateDocumentVerboseResult(results);
                    } else {
                        parser.skipChildren();
                    }
                } else if (token.equals(Token.START_OBJECT)) {
                    switch (fieldName) {
                        case WriteableIngestDocument.DOC_FIELD -> result = new SimulateDocumentBaseResult(
                            WriteableIngestDocument.INGEST_DOC_PARSER.apply(parser, null).getIngestDocument()
                        );
                        case "error" -> result = new SimulateDocumentBaseResult(ElasticsearchException.fromXContent(parser));
                        default -> parser.skipChildren();
                    }
                } // else it is a value skip it
            }
            assert result != null;
            return result;
        }, new ParseField(Fields.DOCUMENTS));
    }

    public SimulatePipelineResponse(StreamInput in) throws IOException {
        super(in);
        this.pipelineId = in.readOptionalString();
        boolean verbose = in.readBoolean();
        int responsesLength = in.readVInt();
        results = new ArrayList<>();
        for (int i = 0; i < responsesLength; i++) {
            SimulateDocumentResult simulateDocumentResult;
            if (verbose) {
                simulateDocumentResult = new SimulateDocumentVerboseResult(in);
            } else {
                simulateDocumentResult = new SimulateDocumentBaseResult(in);
            }
            results.add(simulateDocumentResult);
        }
    }

    public SimulatePipelineResponse(String pipelineId, boolean verbose, List<SimulateDocumentResult> responses) {
        this.pipelineId = pipelineId;
        this.verbose = verbose;
        this.results = Collections.unmodifiableList(responses);
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public List<SimulateDocumentResult> getResults() {
        return results;
    }

    public boolean isVerbose() {
        return verbose;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(pipelineId);
        out.writeBoolean(verbose);
        out.writeCollection(results);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(Fields.DOCUMENTS);
        for (SimulateDocumentResult response : results) {
            response.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static SimulatePipelineResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    static final class Fields {
        static final String DOCUMENTS = "docs";
    }
}
