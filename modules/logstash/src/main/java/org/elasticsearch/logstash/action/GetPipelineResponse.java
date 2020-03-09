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

package org.elasticsearch.logstash.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class GetPipelineResponse extends ActionResponse implements ToXContentObject {

    private final Map<String, BytesReference> pipelines;

    public GetPipelineResponse(Map<String, BytesReference> pipelines) {
        this.pipelines = pipelines;
    }

    public GetPipelineResponse(StreamInput in) throws IOException {
        super(in);
        this.pipelines = in.readMap(StreamInput::readString, StreamInput::readBytesReference);
    }

    public Map<String, BytesReference> pipelines() {
        return pipelines;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(pipelines, StreamOutput::writeString, StreamOutput::writeBytesReference);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Entry<String, BytesReference> entry : pipelines.entrySet()) {
            builder.rawField(entry.getKey(), entry.getValue().streamInput());
        }
        return builder.endObject();
    }
}
