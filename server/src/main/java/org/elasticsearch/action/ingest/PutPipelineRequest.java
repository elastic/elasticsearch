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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

public class PutPipelineRequest extends AcknowledgedRequest<PutPipelineRequest> implements ToXContentObject {

    private String id;
    private BytesReference source;
    private XContentType xContentType;

    /**
     * Create a new pipeline request with the id and source along with the content type of the source
     */
    public PutPipelineRequest(String id, BytesReference source, XContentType xContentType) {
        this.id = Objects.requireNonNull(id);
        this.source = Objects.requireNonNull(source);
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    public PutPipelineRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        source = in.readBytesReference();
        xContentType = in.readEnum(XContentType.class);
    }

    PutPipelineRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getId() {
        return id;
    }

    public BytesReference getSource() {
        return source;
    }

    public XContentType getXContentType() {
        return xContentType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeBytesReference(source);
        out.writeEnum(xContentType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (source != null) {
            builder.rawValue(source.streamInput(), xContentType);
        } else {
            builder.startObject().endObject();
        }
        return builder;
    }
}
