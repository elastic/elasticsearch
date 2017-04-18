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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SimulatePipelineResponse extends ActionResponse implements ToXContentObject {
    private String pipelineId;
    private boolean verbose;
    private List<SimulateDocumentResult> results;

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

    static final class Fields {
        static final String DOCUMENTS = "docs";
    }
}
