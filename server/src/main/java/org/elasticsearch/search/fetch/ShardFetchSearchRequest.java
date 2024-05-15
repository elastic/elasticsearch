/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rank.RankDoc;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Shard level fetch request used with search. Holds indices taken from the original search request
 * and implements {@link org.elasticsearch.action.IndicesRequest}.
 */
public class ShardFetchSearchRequest extends ShardFetchRequest implements IndicesRequest {

    private final OriginalIndices originalIndices;
    private final ShardSearchRequest shardSearchRequest;
    private final RescoreDocIds rescoreDocIds;
    private final AggregatedDfs aggregatedDfs;
    private final Map<Integer, RankDoc> rankDocs;

    public ShardFetchSearchRequest(
        OriginalIndices originalIndices,
        ShardSearchContextId id,
        ShardSearchRequest shardSearchRequest,
        List<Integer> docIds,
        Map<Integer, RankDoc> rankDocs,
        ScoreDoc lastEmittedDoc,
        RescoreDocIds rescoreDocIds,
        AggregatedDfs aggregatedDfs
    ) {
        super(id, docIds, lastEmittedDoc);
        this.originalIndices = originalIndices;
        this.shardSearchRequest = shardSearchRequest;
        this.rescoreDocIds = rescoreDocIds;
        this.aggregatedDfs = aggregatedDfs;
        this.rankDocs = rankDocs;
    }

    public ShardFetchSearchRequest(StreamInput in) throws IOException {
        super(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
        shardSearchRequest = in.readOptionalWriteable(ShardSearchRequest::new);
        rescoreDocIds = new RescoreDocIds(in);
        aggregatedDfs = in.readOptionalWriteable(AggregatedDfs::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.RANK_DOC_IN_SHARD_FETCH_REQUEST)) {
            boolean hasShardDocs = in.readBoolean();
            if (hasShardDocs) {
                this.rankDocs = in.readMap(StreamInput::readVInt, v -> in.readNamedWriteable(RankDoc.class));
            } else {
                this.rankDocs = null;
            }
        } else {
            rankDocs = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        out.writeOptionalWriteable(shardSearchRequest);
        rescoreDocIds.writeTo(out);
        out.writeOptionalWriteable(aggregatedDfs);
        if (out.getTransportVersion().onOrAfter(TransportVersions.RANK_DOC_IN_SHARD_FETCH_REQUEST)) {
            if (rankDocs != null) {
                out.writeBoolean(true);
                out.writeMap(rankDocs, StreamOutput::writeVInt, StreamOutput::writeNamedWriteable);
            } else {
                out.writeBoolean(false);
            }
        }
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

    @Override
    public Map<Integer, RankDoc> getRankDocks() {
        return this.rankDocs;
    }
}
