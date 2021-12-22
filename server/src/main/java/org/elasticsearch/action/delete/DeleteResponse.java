/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.delete;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * The response of the delete action.
 *
 * @see org.elasticsearch.action.delete.DeleteRequest
 * @see org.elasticsearch.client.internal.Client#delete(DeleteRequest)
 */
public class DeleteResponse extends DocWriteResponse {

    public DeleteResponse(ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
    }

    public DeleteResponse(StreamInput in) throws IOException {
        super(in);
    }

    public DeleteResponse(ShardId shardId, String id, long seqNo, long primaryTerm, long version, boolean found) {
        this(shardId, id, seqNo, primaryTerm, version, found ? Result.DELETED : Result.NOT_FOUND);
    }

    private DeleteResponse(ShardId shardId, String id, long seqNo, long primaryTerm, long version, Result result) {
        super(shardId, id, seqNo, primaryTerm, version, assertDeletedOrNotFound(result));
    }

    private static Result assertDeletedOrNotFound(Result result) {
        assert result == Result.DELETED || result == Result.NOT_FOUND;
        return result;
    }

    @Override
    public RestStatus status() {
        return result == Result.DELETED ? super.status() : RestStatus.NOT_FOUND;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DeleteResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",result=").append(getResult().getLowercase());
        builder.append(",shards=").append(getShardInfo());
        return builder.append("]").toString();
    }

    public static DeleteResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        Builder context = new Builder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parseXContentFields(parser, context);
        }
        return context.build();
    }

    /**
     * Parse the current token and update the parsing context appropriately.
     */
    public static void parseXContentFields(XContentParser parser, Builder context) throws IOException {
        DocWriteResponse.parseInnerToXContent(parser, context);
    }

    /**
     * Builder class for {@link DeleteResponse}. This builder is usually used during xcontent parsing to
     * temporarily store the parsed values, then the {@link DocWriteResponse.Builder#build()} method is called to
     * instantiate the {@link DeleteResponse}.
     */
    public static class Builder extends DocWriteResponse.Builder {

        @Override
        public DeleteResponse build() {
            DeleteResponse deleteResponse = new DeleteResponse(shardId, id, seqNo, primaryTerm, version, result);
            deleteResponse.setForcedRefresh(forcedRefresh);
            if (shardInfo != null) {
                deleteResponse.setShardInfo(shardInfo);
            }
            return deleteResponse;
        }
    }
}
