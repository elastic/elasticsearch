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

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.action.support.DefaultShardOperationFailedException.readShardOperationFailed;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Base class for all broadcast operation based responses.
 */
public class BroadcastResponse extends ActionResponse implements ToXContentObject {

    public static final DefaultShardOperationFailedException[] EMPTY = new DefaultShardOperationFailedException[0];

    private static final ParseField _SHARDS_FIELD = new ParseField("_shards");
    private static final ParseField TOTAL_FIELD = new ParseField("total");
    private static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");
    private static final ParseField FAILED_FIELD = new ParseField("failed");
    private static final ParseField FAILURES_FIELD = new ParseField("failures");

    private int totalShards;
    private int successfulShards;
    private int failedShards;
    private DefaultShardOperationFailedException[] shardFailures = EMPTY;

    @SuppressWarnings("unchecked")
    protected static <T extends BroadcastResponse> void declareBroadcastFields(ConstructingObjectParser<T, Void> PARSER) {
        ConstructingObjectParser<BroadcastResponse, Void> shardsParser = new ConstructingObjectParser<>("_shards", true,
            arg -> new BroadcastResponse((int) arg[0], (int) arg[1], (int) arg[2], (List<DefaultShardOperationFailedException>) arg[3]));
        shardsParser.declareInt(constructorArg(), TOTAL_FIELD);
        shardsParser.declareInt(constructorArg(), SUCCESSFUL_FIELD);
        shardsParser.declareInt(constructorArg(), FAILED_FIELD);
        shardsParser.declareObjectArray(optionalConstructorArg(),
            (p, c) -> DefaultShardOperationFailedException.fromXContent(p), FAILURES_FIELD);
        PARSER.declareObject(constructorArg(), shardsParser, _SHARDS_FIELD);
    }

    public BroadcastResponse() {}

    public BroadcastResponse(StreamInput in) throws IOException {
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        failedShards = in.readVInt();
        int size = in.readVInt();
        if (size > 0) {
            shardFailures = new DefaultShardOperationFailedException[size];
            for (int i = 0; i < size; i++) {
                shardFailures[i] = readShardOperationFailed(in);
            }
        }
    }

    public BroadcastResponse(int totalShards, int successfulShards, int failedShards,
                             List<DefaultShardOperationFailedException> shardFailures) {
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.failedShards = failedShards;
        if (shardFailures == null) {
            this.shardFailures = EMPTY;
        } else {
            this.shardFailures = shardFailures.toArray(new DefaultShardOperationFailedException[shardFailures.size()]);
        }
    }

    /**
     * The total shards this request ran against.
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful shards this request was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * The failed shards this request was executed on.
     */
    public int getFailedShards() {
        return failedShards;
    }

    /**
     * The REST status that should be used for the response
     */
    public RestStatus getStatus() {
        if (failedShards > 0) {
            return shardFailures[0].status();
        } else {
            return RestStatus.OK;
        }
    }

    /**
     * The list of shard failures exception.
     */
    public DefaultShardOperationFailedException[] getShardFailures() {
        return shardFailures;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(failedShards);
        out.writeVInt(shardFailures.length);
        for (DefaultShardOperationFailedException exp : shardFailures) {
            exp.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        RestActions.buildBroadcastShardsHeader(builder, params, this);
        addCustomXContentFields(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Override in subclass to add custom fields following the common `_shards` field
     */
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
    }
}
