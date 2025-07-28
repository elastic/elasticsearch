/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank.feature;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Shard level request for extracting all needed feature for a global reranker
 */

public class RankFeatureShardRequest extends AbstractTransportRequest implements IndicesRequest {

    private final OriginalIndices originalIndices;
    private final ShardSearchRequest shardSearchRequest;

    private final ShardSearchContextId contextId;

    private final int[] docIds;

    public RankFeatureShardRequest(
        OriginalIndices originalIndices,
        ShardSearchContextId contextId,
        ShardSearchRequest shardSearchRequest,
        List<Integer> docIds
    ) {
        this.originalIndices = originalIndices;
        this.shardSearchRequest = shardSearchRequest;
        this.docIds = docIds.stream().flatMapToInt(IntStream::of).toArray();
        this.contextId = contextId;
    }

    public RankFeatureShardRequest(StreamInput in) throws IOException {
        super(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
        shardSearchRequest = in.readOptionalWriteable(ShardSearchRequest::new);
        docIds = in.readIntArray();
        contextId = in.readOptionalWriteable(ShardSearchContextId::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        out.writeOptionalWriteable(shardSearchRequest);
        out.writeIntArray(docIds);
        out.writeOptionalWriteable(contextId);
    }

    @Override
    public String[] indices() {
        if (originalIndices == null) {
            return null;
        }
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        if (originalIndices == null) {
            return null;
        }
        return originalIndices.indicesOptions();
    }

    public ShardSearchRequest getShardSearchRequest() {
        return shardSearchRequest;
    }

    public int[] getDocIds() {
        return docIds;
    }

    public ShardSearchContextId contextId() {
        return contextId;
    }

    @Override
    public SearchShardTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }
}
