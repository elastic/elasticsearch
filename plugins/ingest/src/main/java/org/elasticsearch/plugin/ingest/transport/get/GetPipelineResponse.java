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

package org.elasticsearch.plugin.ingest.transport.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GetPipelineResponse extends ActionResponse implements StatusToXContent {

    private Map<String, BytesReference> pipelines;
    private Map<String, Long> versions;

    public GetPipelineResponse() {
    }

    public GetPipelineResponse(Map<String, BytesReference> pipelines, Map<String, Long> versions) {
        this.pipelines = pipelines;
        this.versions = versions;
    }

    public Map<String, BytesReference> pipelines() {
        return pipelines;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        pipelines = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            pipelines.put(in.readString(), in.readBytesReference());
        }
        size = in.readVInt();
        versions = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            versions.put(in.readString(), in.readVLong());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(pipelines.size());
        for (Map.Entry<String, BytesReference> entry : pipelines.entrySet()) {
            out.writeString(entry.getKey());
            out.writeBytesReference(entry.getValue());
        }
        out.writeVInt(versions.size());
        for (Map.Entry<String, Long> entry : versions.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVLong(entry.getValue());
        }
    }

    public boolean isFound() {
        return !pipelines.isEmpty();
    }

    @Override
    public RestStatus status() {
        return isFound() ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, BytesReference> entry : pipelines.entrySet()) {
            builder.startObject(entry.getKey());
            XContentHelper.writeRawField("_source", entry.getValue(), builder, params);
            builder.field("_version", versions.get(entry.getKey()));
            builder.endObject();
        }
        return builder;
    }
}
