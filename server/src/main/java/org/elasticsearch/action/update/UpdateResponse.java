/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.update;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class UpdateResponse extends DocWriteResponse {

    static final String GET = "get";

    private GetResult getResult;

    public UpdateResponse(ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
        if (in.readBoolean()) {
            getResult = new GetResult(in);
        }
    }

    public UpdateResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            getResult = new GetResult(in);
        }
    }

    /**
     * Constructor to be used when a update didn't translate in a write.
     * For example: update script with operation set to none
     */
    public UpdateResponse(ShardId shardId, String id, long seqNo, long primaryTerm, long version, Result result) {
        this(ShardInfo.EMPTY, shardId, id, seqNo, primaryTerm, version, result);
    }

    @SuppressWarnings("this-escape")
    public UpdateResponse(ShardInfo shardInfo, ShardId shardId, String id, long seqNo, long primaryTerm, long version, Result result) {
        super(shardId, id, seqNo, primaryTerm, version, result);
        setShardInfo(shardInfo);
    }

    public void setGetResult(GetResult getResult) {
        this.getResult = getResult;
    }

    public GetResult getGetResult() {
        return this.getResult;
    }

    @Override
    public RestStatus status() {
        return this.result == Result.CREATED ? RestStatus.CREATED : super.status();
    }

    @Override
    public void writeThin(StreamOutput out) throws IOException {
        super.writeThin(out);
        writeGetResult(out);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeGetResult(out);
    }

    private void writeGetResult(StreamOutput out) throws IOException {
        if (getResult == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            getResult.writeTo(out);
        }
    }

    @Override
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerToXContent(builder, params);
        if (getGetResult() != null) {
            builder.startObject(GET);
            getGetResult().toXContentEmbedded(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("UpdateResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",seqNo=").append(getSeqNo());
        builder.append(",primaryTerm=").append(getPrimaryTerm());
        builder.append(",result=").append(getResult().getLowercase());
        builder.append(",shards=").append(getShardInfo());
        return builder.append("]").toString();
    }

    /**
     * Builder class for {@link UpdateResponse}. This builder is usually used during xcontent parsing to
     * temporarily store the parsed values, then the {@link DocWriteResponse.Builder#build()} method is called to
     * instantiate the {@link UpdateResponse}.
     */
    public static class Builder extends DocWriteResponse.Builder {

        private GetResult getResult = null;

        public void setGetResult(GetResult getResult) {
            this.getResult = getResult;
        }

        @Override
        public UpdateResponse build() {
            UpdateResponse update;
            if (shardInfo != null) {
                update = new UpdateResponse(shardInfo, shardId, id, seqNo, primaryTerm, version, result);
            } else {
                update = new UpdateResponse(shardId, id, seqNo, primaryTerm, version, result);
            }
            if (getResult != null) {
                update.setGetResult(
                    new GetResult(
                        update.getIndex(),
                        update.getId(),
                        getResult.getSeqNo(),
                        getResult.getPrimaryTerm(),
                        update.getVersion(),
                        getResult.isExists(),
                        getResult.internalSourceRef(),
                        getResult.getDocumentFields(),
                        getResult.getMetadataFields()
                    )
                );
            }
            update.setForcedRefresh(forcedRefresh);
            return update;
        }
    }
}
