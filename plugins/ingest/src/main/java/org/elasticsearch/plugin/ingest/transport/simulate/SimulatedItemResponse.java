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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.ingest.Data;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class SimulatedItemResponse implements Streamable, StatusToXContent {

    private Data data;
    private Throwable failure;

    public SimulatedItemResponse() {

    }

    public SimulatedItemResponse(Data data) {
        this.data = data;
    }

    public SimulatedItemResponse(Throwable failure) {
        this.failure = failure;
    }

    public boolean failed() {
        return this.failure != null;
    }

    public Data getData() {
        return data;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        boolean failed = in.readBoolean();

        if (failed) {
            this.failure = in.readThrowable();
            // TODO(talevy): check out mget for throwable limitations
        } else {
            String index = in.readString();
            String type = in.readString();
            String id = in.readString();
            Map<String, Object> doc = in.readMap();
            this.data = new Data(index, type, id, doc);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(failed());

        if (failed()) {
            out.writeThrowable(failure);
        } else {
            out.writeString(data.getIndex());
            out.writeString(data.getType());
            out.writeString(data.getId());
            out.writeMap(data.getDocument());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.ERROR, failed());
        if (failed()) {
            builder.field(Fields.FAILURE, failure.toString());
        } else {
            builder.field(Fields.MODIFIED, data.isModified());
            builder.field(Fields.DOCUMENT, data.getDocument());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public RestStatus status() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) { return true; }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SimulatedItemResponse other = (SimulatedItemResponse) obj;
        return Objects.equals(data, other.data) && Objects.equals(failure, other.failure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, failure);
    }

    static final class Fields {
        static final XContentBuilderString DOCUMENT = new XContentBuilderString("doc");
        static final XContentBuilderString ERROR = new XContentBuilderString("error");
        static final XContentBuilderString FAILURE = new XContentBuilderString("failure");
        static final XContentBuilderString MODIFIED = new XContentBuilderString("modified");
    }
}
