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

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * A response of an index operation,
 *
 * @see org.elasticsearch.action.index.IndexRequest
 * @see org.elasticsearch.client.Client#index(IndexRequest)
 */
public class IndexResponse extends DocWriteResponse {

    private boolean created;

    public IndexResponse() {

    }

    public IndexResponse(ShardId shardId, String type, String id, long version, boolean created) {
        super(shardId, type, id, version);
        this.created = created;
    }

    /**
     * Returns true if the document was created, false if updated.
     */
    public boolean isCreated() {
        return this.created;
    }

    @Override
    public RestStatus status() {
        if (created) {
            return RestStatus.CREATED;
        }
        return super.status();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        created = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(created);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",type=").append(getType());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",created=").append(created);
        builder.append(",shards=").append(getShardInfo());
        return builder.append("]").toString();
    }

    static final class Fields {
        static final XContentBuilderString CREATED = new XContentBuilderString("created");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        super.toXContent(builder, params);
        builder.field(Fields.CREATED, isCreated());
        return builder;
    }
}
