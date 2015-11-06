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

package org.elasticsearch.plugin.ingest.transport.simulate;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SimulatePipelineResponse extends ActionResponse implements StatusToXContent {

    private String pipelineId;
    private List<SimulatedItemResponse> responses;

    public SimulatePipelineResponse() {

    }

    public SimulatePipelineResponse(String pipelineId, List<SimulatedItemResponse> responses) {
        this.pipelineId = pipelineId;
        this.responses = Collections.unmodifiableList(responses);
    }

    public String pipelineId() {
        return pipelineId;
    }

    public void pipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
    }

    public List<SimulatedItemResponse> responses() {
        return responses;
    }

    public void responses(List<SimulatedItemResponse> responses) {
        this.responses = responses;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(pipelineId);
        out.writeVInt(responses.size());
        for (SimulatedItemResponse response : responses) {
            response.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.pipelineId = in.readString();
        int responsesLength = in.readVInt();
        responses = new ArrayList<>();
        for (int i = 0; i < responsesLength; i++) {
            SimulatedItemResponse response = new SimulatedItemResponse();
            response.readFrom(in);
            responses.add(response);
        }

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("docs");
        for (SimulatedItemResponse response : responses) {
            builder.value(response);
        }
        builder.endArray();

        return builder;
    }

    @Override
    public RestStatus status() {
        for (SimulatedItemResponse response : responses) {
            if (response.failed()) {
                return RestStatus.BAD_REQUEST;
            }
        }
        return RestStatus.OK;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimulatePipelineResponse that = (SimulatePipelineResponse) o;
        return Objects.equals(pipelineId, that.pipelineId) &&
                Objects.equals(responses, that.responses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pipelineId, responses);
    }
}
