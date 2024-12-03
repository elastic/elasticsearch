/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank.feature;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.SearchContextSourcePrinter;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.Arrays;
import java.util.Collections;

/**
 * The {@code RankFeatureShardPhase} executes the rank feature phase on the shard, iff there is a {@code RankBuilder} that requires it.
 * This phase is responsible for reading field data for a set of docids. To do this, it reuses the {@code FetchPhase} to read the required
 * fields for all requested documents using the `FetchFieldPhase` sub-phase.
 */
public final class RankFeatureShardPhase {

    private static final Logger logger = LogManager.getLogger(RankFeatureShardPhase.class);

    public static final RankFeatureShardResult EMPTY_RESULT = new RankFeatureShardResult(new RankFeatureDoc[0]);

    private RankFeatureShardPhase() {}

    public static void prepareForFetch(SearchContext searchContext, RankFeatureShardRequest request) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}", new SearchContextSourcePrinter(searchContext));
        }

        if (searchContext.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }

        RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext = shardContext(searchContext);
        if (rankFeaturePhaseRankShardContext != null) {
            assert rankFeaturePhaseRankShardContext.getField() != null : "field must not be null";
            searchContext.fetchFieldsContext(
                new FetchFieldsContext(Collections.singletonList(new FieldAndFormat(rankFeaturePhaseRankShardContext.getField(), null)))
            );
            searchContext.storedFieldsContext(StoredFieldsContext.fromList(Collections.singletonList(StoredFieldsContext._NONE_)));
            searchContext.addFetchResult();
            Arrays.sort(request.getDocIds());
        }
    }

    public static void processFetch(SearchContext searchContext) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}", new SearchContextSourcePrinter(searchContext));
        }

        if (searchContext.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }

        RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext = searchContext.request().source().rankBuilder() != null
            ? searchContext.request().source().rankBuilder().buildRankFeaturePhaseShardContext()
            : null;
        if (rankFeaturePhaseRankShardContext != null) {
            // TODO: here we populate the profile part of the fetchResult as well
            // we need to see what info we want to include on the overall profiling section. This is something that is per-shard
            // so most likely we will still care about the `FetchFieldPhase` profiling info as we could potentially
            // operate on `rank_window_size` instead of just `size` results, so this could be much more expensive.
            FetchSearchResult fetchSearchResult = searchContext.fetchResult();
            if (fetchSearchResult == null || fetchSearchResult.hits() == null) {
                return;
            }
            // this cannot be null; as we have either already checked for it, or we would have thrown in
            // FetchSearchResult#shardResult()
            SearchHits hits = fetchSearchResult.hits();
            RankFeatureShardResult featureRankShardResult = (RankFeatureShardResult) rankFeaturePhaseRankShardContext
                .buildRankFeatureShardResult(hits, searchContext.shardTarget().getShardId().id());
            // save the result in the search context
            // need to add profiling info as well available from fetch
            if (featureRankShardResult != null) {
                searchContext.rankFeatureResult().shardResult(featureRankShardResult);
            }
        }
    }

    private static RankFeaturePhaseRankShardContext shardContext(SearchContext searchContext) {
        return searchContext.request().source() != null && searchContext.request().source().rankBuilder() != null
            ? searchContext.request().source().rankBuilder().buildRankFeaturePhaseShardContext()
            : null;
    }
}
