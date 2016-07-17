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
package org.elasticsearch.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Locale;

/**
 * A base class for the response of a write operation that involves a single doc
 */
public abstract class DocWriteResponse extends ReplicationResponse implements WriteResponse, StatusToXContent {

    public enum Operation {
        CREATE(0),
        INDEX(1),
        DELETE(2),
        NOOP(3);

        private final byte op;
        private final String lowercase;

        Operation(int op) {
            this.op = (byte) op;
            this.lowercase = this.toString().toLowerCase(Locale.ENGLISH);
        }

        public byte getOp() {
            return op;
        }

        public String getLowercase(){
            return lowercase;
        }

        public static final Operation formOperation(byte opcode) {
            switch(opcode) {
                case 0:
                    return CREATE;
                case 1:
                    return INDEX;
                case 2:
                    return DELETE;
                case 3:
                    return NOOP;
                default:
                    throw new IllegalArgumentException("Unknown operation code: " + opcode);
            }
        }
    }

    private ShardId shardId;
    private String id;
    private String type;
    private long version;
    private boolean forcedRefresh;
    protected Operation operation;

    public DocWriteResponse(ShardId shardId, String type, String id, long version, Operation operation) {
        this.shardId = shardId;
        this.type = type;
        this.id = id;
        this.version = version;
        this.operation = operation;
    }

    // needed for deserialization
    protected DocWriteResponse() {
    }

    /**
     * The change that occurred to the document.
     */
    public Operation getOperation() {
        return operation;
    }

    /**
     * The index the document was changed in.
     */
    public String getIndex() {
        return this.shardId.getIndexName();
    }


    /**
     * The exact shard the document was changed in.
     */
    public ShardId getShardId() {
        return this.shardId;
    }

    /**
     * The type of the document changed.
     */
    public String getType() {
        return this.type;
    }

    /**
     * The id of the document changed.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Returns the current version of the doc.
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Did this request force a refresh? Requests that set {@link WriteRequest#setRefreshPolicy(RefreshPolicy)} to
     * {@link RefreshPolicy#IMMEDIATE} will always return true for this. Requests that set it to {@link RefreshPolicy#WAIT_UNTIL} will
     * only return true here if they run out of refresh listener slots (see {@link IndexSettings#MAX_REFRESH_LISTENERS_PER_SHARD}).
     */
    public boolean forcedRefresh() {
        return forcedRefresh;
    }

    @Override
    public void setForcedRefresh(boolean forcedRefresh) {
        this.forcedRefresh = forcedRefresh;
    }

    /** returns the rest status for this response (based on {@link ShardInfo#status()} */
    public RestStatus status() {
        return getShardInfo().status();
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = ShardId.readShardId(in);
        type = in.readString();
        id = in.readString();
        version = in.readZLong();
        forcedRefresh = in.readBoolean();
        operation = Operation.formOperation(in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeString(type);
        out.writeString(id);
        out.writeZLong(version);
        out.writeBoolean(forcedRefresh);
        out.writeByte(getOperation().getOp());
    }

    static final class Fields {
        static final String _INDEX = "_index";
        static final String _TYPE = "_type";
        static final String _ID = "_id";
        static final String _VERSION = "_version";
        static final String _OPERATION = "_operation";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        ReplicationResponse.ShardInfo shardInfo = getShardInfo();
        builder.field(Fields._INDEX, shardId.getIndexName())
            .field(Fields._TYPE, type)
            .field(Fields._ID, id)
            .field(Fields._VERSION, version)
            .field(Fields._OPERATION, getOperation().getLowercase())
            .field("forced_refresh", forcedRefresh);
        shardInfo.toXContent(builder, params);
        return builder;
    }
}
