/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;// package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.ArrayList;
import java.util.List;

/**
 *  Coordinator accumulates the chunks sent from the data nodes (in-memory)
 */
class FetchPhaseResponseStream extends AbstractRefCounted {

    private static final Logger logger = LogManager.getLogger(FetchPhaseResponseStream.class);

    private final int shardIndex;
    private final int expectedDocs;
    private final List<SearchHit> hits = new ArrayList<>();
    // or a fixed-size array indexed by "from + offset" if you want exact slot mapping

    FetchPhaseResponseStream(int shardIndex, int expectedDocs) {
        //super("fetch_phase_accumulator_" + shardIndex);
        this.shardIndex = shardIndex;
        this.expectedDocs = expectedDocs;
    }

    void startResponse(Releasable releasable) {
        // you can sanity-check expectedDocs etc
    }

    void writeChunk(FetchPhaseResponseChunk chunk, Releasable releasable) {
        try (releasable) {
            if (chunk.hits() != null) {
                for (SearchHit hit : chunk.hits().getHits()) {
                    hits.add(hit);
                }
            }
        }
    }

    FetchSearchResult buildFinalResult(ShardSearchContextId ctxId, SearchShardTarget shardTarget) {
        // construct a FetchSearchResult matching the usual semantics
        FetchSearchResult result = new FetchSearchResult(ctxId, shardTarget);
        SearchHits searchHits = new SearchHits(
            hits.toArray(SearchHit[]::new),
            new TotalHits(hits.size(), TotalHits.Relation.EQUAL_TO),
            Float.NaN
        );
        result.shardResult(searchHits, /* profile */ null);
        return result;
    }

    @Override
    protected void closeInternal() {
        hits.clear();
    }
}
