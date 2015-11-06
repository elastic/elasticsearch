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

public class SimulatePipelineResponse extends ActionResponse implements StatusToXContent {

    private String pipelineId;
    private SimulatedItemResponse[] responses;

    public String pipelineId() {
        return pipelineId;
    }

    public SimulatePipelineResponse pipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
        return this;
    }

    public SimulatePipelineResponse responses(SimulatedItemResponse[] responses) {
        this.responses = responses;
        return this;
    }

    public SimulatedItemResponse[] responses() {
        return responses;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(pipelineId);
        out.writeVInt(responses.length);
        for (SimulatedItemResponse response : responses) {
            response.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.pipelineId = in.readString();
        int responsesLength = in.readVInt();
        responses = new SimulatedItemResponse[responsesLength];
        for (int i = 0; i < responsesLength; i++) {
            SimulatedItemResponse response = new SimulatedItemResponse();
            response.readFrom(in);
            responses[i] = response;
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
                return response.status();
            }
        }
        return RestStatus.OK;
    }
}
