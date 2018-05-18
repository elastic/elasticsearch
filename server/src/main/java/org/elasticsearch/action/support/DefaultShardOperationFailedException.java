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

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.ExceptionsHelper.detailedMessage;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class DefaultShardOperationFailedException implements ShardOperationFailedException {

    private static final String INDEX = "index";
    private static final String SHARD_ID = "shard";
    private static final String REASON = "reason";

    private static final ConstructingObjectParser<DefaultShardOperationFailedException, Void> PARSER = new ConstructingObjectParser<>(
        "failures", true, arg -> new DefaultShardOperationFailedException((String) arg[0], (int) arg[1] ,(Throwable) arg[2]));

    static {
        PARSER.declareString(constructorArg(), new ParseField(INDEX));
        PARSER.declareInt(constructorArg(), new ParseField(SHARD_ID));
        PARSER.declareObject(constructorArg(), (p, c) -> ElasticsearchException.fromXContent(p), new ParseField(REASON));
    }

    private String index;

    private int shardId;

    private Throwable reason;

    private RestStatus status;

    protected DefaultShardOperationFailedException() {
    }

    public DefaultShardOperationFailedException(ElasticsearchException e) {
        Index index = e.getIndex();
        this.index = index == null ? null : index.getName();
        ShardId shardId = e.getShardId();
        this.shardId = shardId == null ? -1 : shardId.id();
        this.reason = e;
        this.status = e.status();
    }

    public DefaultShardOperationFailedException(String index, int shardId, Throwable reason) {
        this.index = index;
        this.shardId = shardId;
        this.reason = reason;
        this.status = ExceptionsHelper.status(reason);
    }

    @Override
    public String index() {
        return this.index;
    }

    @Override
    public int shardId() {
        return this.shardId;
    }

    @Override
    public String reason() {
        return detailedMessage(reason);
    }

    @Override
    public RestStatus status() {
        return status;
    }

    @Override
    public Throwable getCause() {
        return reason;
    }

    public static DefaultShardOperationFailedException readShardOperationFailed(StreamInput in) throws IOException {
        DefaultShardOperationFailedException exp = new DefaultShardOperationFailedException();
        exp.readFrom(in);
        return exp;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            index = in.readString();
        }
        shardId = in.readVInt();
        reason = in.readException();
        status = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (index == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(index);
        }
        out.writeVInt(shardId);
        out.writeException(reason);
        RestStatus.writeTo(out, status);
    }

    @Override
    public String toString() {
        return "[" + index + "][" + shardId + "] failed, reason [" + reason() + "]";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("shard", shardId());
        builder.field("index", index());
        builder.field("status", status.name());
        if (reason != null) {
            builder.startObject("reason");
            ElasticsearchException.generateThrowableXContent(builder, params, reason);
            builder.endObject();
        }
        return builder;
    }

    public static DefaultShardOperationFailedException fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
