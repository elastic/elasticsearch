/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.index;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A response of an index operation,
 *
 * @see org.elasticsearch.action.index.IndexRequest
 * @see org.elasticsearch.client.internal.Client#index(IndexRequest)
 */
public class IndexResponse extends DocWriteResponse {

    /*
     * This is the optional list of ingest pipelines that were executed while indexing the data. A null value means that there is no
     * information about the pipelines executed. An empty list means that there were no pipelines executed.
     */
    @Nullable
    protected final List<String> executedPipelines;

    public IndexResponse(ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.PIPELINES_IN_BULK_RESPONSE_ADDED)) {
            executedPipelines = in.readOptionalCollectionAsList(StreamInput::readString);
        } else {
            executedPipelines = null;
        }
    }

    public IndexResponse(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.PIPELINES_IN_BULK_RESPONSE_ADDED)) {
            executedPipelines = in.readOptionalCollectionAsList(StreamInput::readString);
        } else {
            executedPipelines = null;
        }
    }

    public IndexResponse(ShardId shardId, String id, long seqNo, long primaryTerm, long version, boolean created) {
        this(shardId, id, seqNo, primaryTerm, version, created, null);
    }

    public IndexResponse(
        ShardId shardId,
        String id,
        long seqNo,
        long primaryTerm,
        long version,
        boolean created,
        @Nullable List<String> executedPipelines
    ) {
        this(shardId, id, seqNo, primaryTerm, version, created ? Result.CREATED : Result.UPDATED, executedPipelines);
    }

    private IndexResponse(
        ShardId shardId,
        String id,
        long seqNo,
        long primaryTerm,
        long version,
        Result result,
        @Nullable List<String> executedPipelines
    ) {
        super(shardId, id, seqNo, primaryTerm, version, assertCreatedOrUpdated(result));
        this.executedPipelines = executedPipelines;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.PIPELINES_IN_BULK_RESPONSE_ADDED)) {
            out.writeOptionalCollection(executedPipelines, StreamOutput::writeString);
        }
    }

    @Override
    public void writeThin(StreamOutput out) throws IOException {
        super.writeThin(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.PIPELINES_IN_BULK_RESPONSE_ADDED)) {
            out.writeOptionalCollection(executedPipelines, StreamOutput::writeString);
        }
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder updatedBuilder = super.innerToXContent(builder, params);
        if (executedPipelines != null) {
            updatedBuilder = updatedBuilder.field("executed_pipelines", executedPipelines.toArray());
        }
        return updatedBuilder;
    }

    private static Result assertCreatedOrUpdated(Result result) {
        assert result == Result.CREATED || result == Result.UPDATED;
        return result;
    }

    @Override
    public RestStatus status() {
        return result == Result.CREATED ? RestStatus.CREATED : super.status();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",result=").append(getResult().getLowercase());
        builder.append(",seqNo=").append(getSeqNo());
        builder.append(",primaryTerm=").append(getPrimaryTerm());
        builder.append(",shards=").append(Strings.toString(getShardInfo()));
        return builder.append("]").toString();
    }

    public static IndexResponse fromXContent(XContentParser parser) throws IOException {
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
     * Builder class for {@link IndexResponse}. This builder is usually used during xcontent parsing to
     * temporarily store the parsed values, then the {@link Builder#build()} method is called to
     * instantiate the {@link IndexResponse}.
     */
    public static class Builder extends DocWriteResponse.Builder {
        @Override
        public IndexResponse build() {
            IndexResponse indexResponse = new IndexResponse(shardId, id, seqNo, primaryTerm, version, result, null);
            indexResponse.setForcedRefresh(forcedRefresh);
            if (shardInfo != null) {
                indexResponse.setShardInfo(shardInfo);
            }
            return indexResponse;
        }
    }
}
