/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.ShardRestoringException;
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

    /**
     * With partial results disallowed, rejects the search if any shard has no copy to search. {@code clusterState} is used to find out
     * whether the missing shards are being restored: when every missing shard belongs to a restore that asked for its shards to be
     * reported, the thrown {@link SearchPhaseExecutionException} carries a {@link ShardRestoringException} as its cause, so its status is
     * HTTP 403 instead of 503. Pass {@code null} for {@code clusterState} to always get the 503, for callers that have no cluster state.
     */
    protected static void doCheckNoMissingShards(
        String phaseName,
        SearchRequest request,
        List<SearchShardIterator> shardsIts,
        @Nullable ClusterState clusterState
    ) {
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
                throw new SearchPhaseExecutionException(
                    phaseName,
                    msg,
                    restoringCause(shardsIts, clusterState),
                    ShardSearchFailure.EMPTY_ARRAY
                );
            }
        }
    }

    /**
     * Returns the {@link ShardRestoringException} to reject a search with when every shard that has no copy to search is being restored
     * by a restore that reports its shards, and {@code null} otherwise: if any shard is missing for another reason, waiting for the
     * restores would not let the search proceed, so the rejection stays the 503 it has always been.
     */
    @Nullable
    private static ShardRestoringException restoringCause(List<SearchShardIterator> shardsIts, @Nullable ClusterState clusterState) {
        ShardRestoringException firstException = null;
        for (SearchShardIterator shardIt : shardsIts) {
            if (shardIt.size() > 0) {
                continue;
            }
            final ShardRestoringException shardException = restoringShardException(shardIt, clusterState);
            if (shardException == null) {
                return null;
            }
            if (firstException == null) {
                firstException = shardException;
            }
        }
        return firstException;
    }

    /**
     * Returns the {@link ShardRestoringException} to report for a shard that has no copy to search, or {@code null} if the shard is not
     * being restored by a restore that asked for its shards to be reported, or if its cluster state is not known.
     *
     * <p>The cluster alias is not checked. A cross-cluster search coordinator holds shards of remote clusters, whose restores are not in
     * its cluster state, but the lookup is by {@link Index}, name and UUID, so a remote shard never matches a local restore and is not
     * reported.
     */
    @Nullable
    protected static ShardRestoringException restoringShardException(SearchShardIterator shardIt, @Nullable ClusterState clusterState) {
        if (clusterState == null) {
            return null;
        }
        final Index index = shardIt.shardId().getIndex();
        return clusterState.metadata()
            .lookupProject(index)
            .map(project -> RestoreService.reportableRestoreException(project.id(), shardIt.shardId(), clusterState))
            .orElse(null);
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
