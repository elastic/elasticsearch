/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.List;

/**
 * Shard level fetch request used with search. Holds indices taken from the original search request
 * and implements {@link org.elasticsearch.action.IndicesRequest}.
 */
public class ShardFetchSearchRequest extends ShardFetchRequest implements IndicesRequest {

    private final OriginalIndices originalIndices;
    private final ShardSearchRequest shardSearchRequest;
    private final RescoreDocIds rescoreDocIds;
    private final AggregatedDfs aggregatedDfs;

    public ShardFetchSearchRequest(
        OriginalIndices originalIndices,
        ShardSearchContextId id,
        ShardSearchRequest shardSearchRequest,
        List<Integer> docIds,
        ScoreDoc lastEmittedDoc,
        RescoreDocIds rescoreDocIds,
        AggregatedDfs aggregatedDfs
    ) {
        super(id, docIds, lastEmittedDoc);
        this.originalIndices = originalIndices;
        this.shardSearchRequest = shardSearchRequest;
        this.rescoreDocIds = rescoreDocIds;
        this.aggregatedDfs = aggregatedDfs;
    }

    public ShardFetchSearchRequest(StreamInput in) throws IOException {
        super(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
        shardSearchRequest = in.readOptionalWriteable(ShardSearchRequest::new);
        rescoreDocIds = new RescoreDocIds(in);
        aggregatedDfs = in.readOptionalWriteable(AggregatedDfs::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        out.writeOptionalWriteable(shardSearchRequest);
        rescoreDocIds.writeTo(out);
        out.writeOptionalWriteable(aggregatedDfs);
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

    @Override
    public ShardSearchRequest getShardSearchRequest() {
        return shardSearchRequest;
    }

    @Override
    public RescoreDocIds getRescoreDocIds() {
        return rescoreDocIds;
    }

    @Override
    public AggregatedDfs getAggregatedDfs() {
        return aggregatedDfs;
    }
}
