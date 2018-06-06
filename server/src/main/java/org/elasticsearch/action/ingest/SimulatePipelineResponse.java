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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class SimulatePipelineResponse extends ActionResponse implements ToXContentObject {
    private String pipelineId;
    private boolean verbose;
    private List<SimulateDocumentResult> results;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SimulatePipelineResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "simulate_pipeline_response",
            true,
            a -> {
                List<SimulateDocumentResult> results = (List<SimulateDocumentResult>)a[0];
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
        PARSER.declareObjectArray(
            constructorArg(),
            (p, c) -> {
                ensureExpectedToken(Token.START_OBJECT, p.currentToken(), p::getTokenLocation);
                boolean isVerbose = false;
                boolean isFirst = true;
                SimulateDocumentResult result = null;
                while (p.nextToken().equals(Token.FIELD_NAME)) {
                    switch (p.currentName()) {
                        case SimulateDocumentVerboseResult.PROCESSOR_RESULT_FIELD:
                            assert isFirst || isVerbose;
                            isVerbose = true;
                            ensureExpectedToken(Token.START_ARRAY, p.nextToken(), p::getTokenLocation);
                            List<SimulateProcessorResult> results = new ArrayList<>();
                            while (p.nextToken().equals(Token.START_OBJECT)) {
                                results.add(SimulateProcessorResult.fromXContent(p));
                            }
                            ensureExpectedToken(Token.END_ARRAY, p.currentToken(), p::getTokenLocation);
                            result = new SimulateDocumentVerboseResult(results);
                            break;
                        case WriteableIngestDocument.DOC_FIELD:
                        case "error":
                            assert !isVerbose;
                            if (p.currentName().equals("error")) {
                                p.nextToken();
                                result = new SimulateDocumentBaseResult(ElasticsearchException.fromXContent(p));
                            } else {
                                result = new SimulateDocumentBaseResult(
                                    WriteableIngestDocument.INGEST_DOC_PARSER.apply(p, null).getIngestDocument()
                                );
                            }
                            ensureExpectedToken(Token.END_OBJECT, p.currentToken(), p::getTokenLocation);
                            break;
                        default:
                            p.skipChildren();
                            break;
                    }
                    isFirst = false;
                }
                ensureExpectedToken(Token.END_OBJECT, p.currentToken(), p::getTokenLocation);
                assert result != null;
                return result;
            },
            new ParseField(Fields.DOCUMENTS));
    }

    public SimulatePipelineResponse() {

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
        super.writeTo(out);
        out.writeOptionalString(pipelineId);
        out.writeBoolean(verbose);
        out.writeVInt(results.size());
        for (SimulateDocumentResult response : results) {
            response.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
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
