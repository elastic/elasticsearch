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

package org.elasticsearch.action.index;

import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.CREATED;

/**
 * A response of an index operation,
 *
 * @see org.elasticsearch.action.index.IndexRequest
 * @see org.elasticsearch.client.Client#index(IndexRequest)
 */
public class IndexResponse extends ActionWriteResponse implements StatusToXContent {

    private String index;
    private String id;
    private String type;
    private long version;
    private boolean created;

    public IndexResponse() {

    }

    public IndexResponse(String index, String type, String id, long version, boolean created) {
        this.index = index;
        this.id = id;
        this.type = type;
        this.version = version;
        this.created = created;
    }

    /**
     * The index the document was indexed into.
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * The type of the document indexed.
     */
    public String getType() {
        return this.type;
    }

    /**
     * The id of the document indexed.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Returns the current version of the doc indexed.
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Returns true if the document was created, false if updated.
     */
    public boolean isCreated() {
        return this.created;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        type = in.readString();
        id = in.readString();
        version = in.readLong();
        created = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeString(type);
        out.writeString(id);
        out.writeLong(version);
        out.writeBoolean(created);
    }

    @Override
    public RestStatus status() {
        RestStatus status = getShardInfo().status();
        if (created) {
            status = CREATED;
        }
        return status;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        ActionWriteResponse.ShardInfo shardInfo = getShardInfo();
        builder.field(Fields._INDEX, index)
            .field(Fields._TYPE, type)
            .field(Fields._ID, id)
            .field(Fields._VERSION, version);
        shardInfo.toXContent(builder, params);
        builder.field(Fields.CREATED, created);
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexResponse[");
        builder.append("index=").append(index);
        builder.append(",type=").append(type);
        builder.append(",id=").append(id);
        builder.append(",version=").append(version);
        builder.append(",created=").append(created);
        builder.append(",shards=").append(getShardInfo());
        return builder.append("]").toString();
    }

    static final class Fields {
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString CREATED = new XContentBuilderString("created");
    }
}
