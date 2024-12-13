/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.index;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.IndexDocFailureStoreStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

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
    private IndexDocFailureStoreStatus failureStoreStatus;

    public IndexResponse(ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            executedPipelines = in.readOptionalCollectionAsList(StreamInput::readString);
        } else {
            executedPipelines = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            failureStoreStatus = IndexDocFailureStoreStatus.read(in);
        } else {
            failureStoreStatus = IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN;
        }
    }

    public IndexResponse(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            executedPipelines = in.readOptionalCollectionAsList(StreamInput::readString);
        } else {
            executedPipelines = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            failureStoreStatus = IndexDocFailureStoreStatus.read(in);
        } else {
            failureStoreStatus = IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN;
        }
    }

    public IndexResponse(ShardId shardId, String id, long seqNo, long primaryTerm, long version, boolean created) {
        this(shardId, id, seqNo, primaryTerm, version, created, null, IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN);
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
        this(
            shardId,
            id,
            seqNo,
            primaryTerm,
            version,
            created ? Result.CREATED : Result.UPDATED,
            executedPipelines,
            IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN
        );
    }

    public IndexResponse(
        ShardId shardId,
        String id,
        long seqNo,
        long primaryTerm,
        long version,
        boolean created,
        @Nullable List<String> executedPipelines,
        IndexDocFailureStoreStatus failureStoreStatus
    ) {
        this(shardId, id, seqNo, primaryTerm, version, created ? Result.CREATED : Result.UPDATED, executedPipelines, failureStoreStatus);
    }

    private IndexResponse(
        ShardId shardId,
        String id,
        long seqNo,
        long primaryTerm,
        long version,
        Result result,
        @Nullable List<String> executedPipelines,
        IndexDocFailureStoreStatus failureStoreStatus
    ) {
        super(shardId, id, seqNo, primaryTerm, version, assertCreatedOrUpdated(result));
        this.executedPipelines = executedPipelines;
        this.failureStoreStatus = failureStoreStatus;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalCollection(executedPipelines, StreamOutput::writeString);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            failureStoreStatus.writeTo(out);
        }
    }

    @Override
    public void writeThin(StreamOutput out) throws IOException {
        super.writeThin(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalCollection(executedPipelines, StreamOutput::writeString);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            failureStoreStatus.writeTo(out);
        }
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder updatedBuilder = super.innerToXContent(builder, params);
        if (executedPipelines != null) {
            updatedBuilder = updatedBuilder.field("executed_pipelines", executedPipelines.toArray());
        }
        failureStoreStatus.toXContent(builder, params);
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

    public void setFailureStoreStatus(IndexDocFailureStoreStatus failureStoreStatus) {
        this.failureStoreStatus = failureStoreStatus;
    }

    @Override
    public IndexDocFailureStoreStatus getFailureStoreStatus() {
        return failureStoreStatus;
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
        builder.append(",failure_store=").append(failureStoreStatus.getLabel());
        return builder.append("]").toString();
    }

    /**
     * Builder class for {@link IndexResponse}. This builder is usually used during xcontent parsing to
     * temporarily store the parsed values, then the {@link Builder#build()} method is called to
     * instantiate the {@link IndexResponse}.
     */
    public static class Builder extends DocWriteResponse.Builder {
        @Override
        public IndexResponse build() {
            IndexResponse indexResponse = new IndexResponse(
                shardId,
                id,
                seqNo,
                primaryTerm,
                version,
                result,
                null,
                IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN
            );
            indexResponse.setForcedRefresh(forcedRefresh);
            if (shardInfo != null) {
                indexResponse.setShardInfo(shardInfo);
            }
            return indexResponse;
        }
    }
}
