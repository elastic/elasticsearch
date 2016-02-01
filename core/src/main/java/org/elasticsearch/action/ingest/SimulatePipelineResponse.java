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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.ingest.core.PipelineFactoryError;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SimulatePipelineResponse extends ActionResponse implements StatusToXContent {
    private String pipelineId;
    private boolean verbose;
    private List<SimulateDocumentResult> results;
    private PipelineFactoryError error;

    public SimulatePipelineResponse() {

    }

    public SimulatePipelineResponse(PipelineFactoryError error) {
        this.error = error;
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

    public boolean isError() {
        return error != null;
    }

    @Override
    public RestStatus status() {
        if (isError()) {
            return RestStatus.BAD_REQUEST;
        }
        return RestStatus.OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(isError());
        if (isError()) {
            error.writeTo(out);
        } else {
            out.writeString(pipelineId);
            out.writeBoolean(verbose);
            out.writeVInt(results.size());
            for (SimulateDocumentResult response : results) {
                response.writeTo(out);
            }
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        boolean isError = in.readBoolean();
        if (isError) {
            error = new PipelineFactoryError();
            error.readFrom(in);
        } else {
            this.pipelineId = in.readString();
            boolean verbose = in.readBoolean();
            int responsesLength = in.readVInt();
            results = new ArrayList<>();
            for (int i = 0; i < responsesLength; i++) {
                SimulateDocumentResult<?> simulateDocumentResult;
                if (verbose) {
                    simulateDocumentResult = SimulateDocumentVerboseResult.readSimulateDocumentVerboseResultFrom(in);
                } else {
                    simulateDocumentResult = SimulateDocumentBaseResult.readSimulateDocumentSimpleResult(in);
                }
                results.add(simulateDocumentResult);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (isError()) {
            error.toXContent(builder, params);
        } else {
            builder.startArray(Fields.DOCUMENTS);
            for (SimulateDocumentResult response : results) {
                response.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString DOCUMENTS = new XContentBuilderString("docs");
    }
}
