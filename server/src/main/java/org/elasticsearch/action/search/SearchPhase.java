/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.Transport;

import java.util.List;
import java.util.Objects;

/**
 * Base class for all individual search phases like collecting distributed frequencies, fetching documents, querying shards.
 */
abstract class SearchPhase {
    private final String name;

    protected SearchPhase(String name) {
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    protected abstract void run();

    /**
     * Returns the phases name.
     */
    public String getName() {
        return name;
    }

    private static String makeMissingShardsError(StringBuilder missingShards) {
        return "Search rejected due to missing shards ["
            + missingShards
            + "]. Consider using `allow_partial_search_results` setting to bypass this error.";
    }

    protected static void doCheckNoMissingShards(String phaseName, SearchRequest request, List<SearchShardIterator> shardsIts) {
        assert request.allowPartialSearchResults() != null : "SearchRequest missing setting for allowPartialSearchResults";
        if (request.allowPartialSearchResults() == false) {
            final StringBuilder missingShards = new StringBuilder();
            // Fail-fast verification of all shards being available
            for (int index = 0; index < shardsIts.size(); index++) {
                final SearchShardIterator shardRoutings = shardsIts.get(index);
                if (shardRoutings.size() == 0) {
                    if (missingShards.isEmpty() == false) {
                        missingShards.append(", ");
                    }
                    missingShards.append(shardRoutings.shardId());
                }
            }
            if (missingShards.isEmpty() == false) {
                // Status red - shard is missing all copies and would produce partial results for an index search
                final String msg = makeMissingShardsError(missingShards);
                throw new SearchPhaseExecutionException(phaseName, msg, null, ShardSearchFailure.EMPTY_ARRAY);
            }
        }
    }

    /**
     * Releases shard targets that are not used in the docsIdsToLoad.
     */
    protected static void releaseIrrelevantSearchContext(SearchPhaseResult searchPhaseResult, AbstractSearchAsyncAction<?> context) {
        // we only release search context that we did not fetch from, if we are not scrolling
        // or using a PIT and if it has at least one hit that didn't make it to the global topDocs
        if (searchPhaseResult == null) {
            return;
        }
        // phaseResult.getContextId() is the same for query & rank feature results
        SearchPhaseResult phaseResult = searchPhaseResult.queryResult() != null
            ? searchPhaseResult.queryResult()
            : searchPhaseResult.rankFeatureResult();
        if (phaseResult != null
            && (phaseResult.hasSearchContext()
                || (phaseResult instanceof QuerySearchResult q && q.isPartiallyReduced() && q.getContextId() != null))
            && context.getRequest().scroll() == null
            && (context.isPartOfPointInTime(phaseResult.getContextId()) == false)) {
            try {
                context.getLogger().trace("trying to release search context [{}]", phaseResult.getContextId());
                SearchShardTarget shardTarget = phaseResult.getSearchShardTarget();
                Transport.Connection connection = context.getConnection(shardTarget.getClusterAlias(), shardTarget.getNodeId());
                context.sendReleaseSearchContext(phaseResult.getContextId(), connection);
            } catch (Exception e) {
                context.getLogger().trace("failed to release context", e);
            }
        }
    }
}
