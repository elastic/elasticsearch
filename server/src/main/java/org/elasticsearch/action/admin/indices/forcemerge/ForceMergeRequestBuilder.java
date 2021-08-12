/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A request to force merge one or more indices. In order to force merge all
 * indices, pass an empty array or {@code null} for the indices.
 * {@link #setMaxNumSegments(int)} allows to control the number of segments to force
 * merge down to. By default, will cause the force merge process to merge down
 * to half the configured number of segments.
 */
public class ForceMergeRequestBuilder
        extends BroadcastOperationRequestBuilder<ForceMergeRequest, ForceMergeResponse, ForceMergeRequestBuilder> {

    public ForceMergeRequestBuilder(ElasticsearchClient client, ForceMergeAction action) {
        super(client, action, new ForceMergeRequest());
    }

    /**
     * Will force merge the index down to &lt;= maxNumSegments. By default, will
     * cause the merge process to merge down to half the configured number of
     * segments.
     */
    public ForceMergeRequestBuilder setMaxNumSegments(int maxNumSegments) {
        request.maxNumSegments(maxNumSegments);
        return this;
    }

    /**
     * Will force merge the specific shard of index down to &lt;= maxNumSegments. By default, will
     * cause the merge process to merge down to half the configured number of
     * segments.
     */
    public ForceMergeRequestBuilder setShardId(int shardId) {
        request.shardId(shardId);
        return this;
    }

    /**
     * Should the merge only expunge deletes from the index, without full merging.
     * Defaults to full merging ({@code false}).
     */
    public ForceMergeRequestBuilder setOnlyExpungeDeletes(boolean onlyExpungeDeletes) {
        request.onlyExpungeDeletes(onlyExpungeDeletes);
        return this;
    }

    /**
     * Should flush be performed after the merge. Defaults to {@code true}.
     */
    public ForceMergeRequestBuilder setFlush(boolean flush) {
        request.flush(flush);
        return this;
    }
}
