/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The internal search shards action, which cross-project search sends to each linked project and transform uses for its checkpoints,
 * against an index whose primary is being restored from a snapshot, on a stateless cluster. It has no REST endpoint on serverless, so it
 * is called directly.
 *
 * <p>When the action runs the can-match phase with partial results disallowed, it fails with a 403 instead of a 503 if the restore asks
 * for its shards to be reported. Otherwise it lists the restoring shard with no node and succeeds, as it always has. It skips that phase
 * when no shard could be skipped: without a query, with a {@code match_all} query, with a suggester, or with aggregations that must visit
 * every document.
 */
public class StatelessRestoringShardSearchShardsIT extends AbstractStatelessRestoringShardIT {

    /**
     * A query that the can-match phase can rewrite, so that the action runs it.
     */
    private static final QueryBuilder QUERY = QueryBuilders.termQuery("field", "value");

    public void testSearchShardsWithoutPartialResultsFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            assertShardRestoringException(expectThrows(SearchPhaseExecutionException.class, () -> searchShards(QUERY, false)), restoreUuid);
        });
    }

    public void testSearchShardsWithoutPartialResultsFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            assertServiceUnavailableException(expectThrows(SearchPhaseExecutionException.class, () -> searchShards(QUERY, false)));
        });
    }

    /**
     * With partial results allowed nothing is rejected, whether or not the restore reports its shards: the restoring shard is listed
     * with no node to send it to, and the caller decides what to do about it.
     */
    public void testSearchShardsWithPartialResultsListsTheRestoringShardWithNoNode() throws Exception {
        whileRestoring(randomBoolean(), restoreUuid -> assertRestoringShardHasNoNode(searchShards(QUERY, true)));
    }

    /**
     * Without a query the action does not run the can-match phase, which is the only part of it that rejects missing shards, so nothing
     * is rejected, whether or not the restore reports its shards.
     */
    public void testSearchShardsWithoutAQueryListsTheRestoringShardWithNoNode() throws Exception {
        whileRestoring(randomBoolean(), restoreUuid -> assertRestoringShardHasNoNode(searchShards(null, false)));
    }

    private static SearchShardsResponse searchShards(QueryBuilder query, boolean allowPartialSearchResults) {
        final var request = new SearchShardsRequest(
            new String[] { RESTORED_INDEX },
            SearchRequest.DEFAULT_INDICES_OPTIONS,
            query,
            null,
            null,
            allowPartialSearchResults,
            null
        );
        return client().execute(TransportSearchShardsAction.TYPE, request).actionGet();
    }

    private static void assertRestoringShardHasNoNode(SearchShardsResponse response) {
        assertThat(response.getGroups(), hasSize(1));
        final SearchShardsGroup group = response.getGroups().iterator().next();
        assertThat(group.shardId().getIndexName(), equalTo(RESTORED_INDEX));
        assertThat(group.allocatedNodes(), empty());
    }
}
