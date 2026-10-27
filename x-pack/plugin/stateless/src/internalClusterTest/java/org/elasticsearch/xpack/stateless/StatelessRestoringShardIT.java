/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.ShardRestoringException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Reads and searches against an index whose primary is being restored from a snapshot, on a stateless cluster, where the restore asked for
 * its shards to be reported.
 */
public class StatelessRestoringShardIT extends AbstractStatelessRestoringShardIT {

    /**
     * With partial results disallowed, a search over the restoring index is rejected with a 403 that says which restore it is waiting for.
     */
    public void testSearchWithoutPartialResultsFailsWithShardRestoring() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            final var e = expectThrows(
                SearchPhaseExecutionException.class,
                prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(false)
            );

            assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
            assertThat(shardRestoringCause(e).recoveryId(), equalTo(restoreUuid));
        } finally {
            finishRestore();
        }
    }

    /**
     * With partial results allowed, searching only the restoring index fails because no shard can answer, and the failure is the
     * restoring one.
     */
    public void testSearchOfOnlyTheRestoringIndexFailsWithShardRestoring() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            final var e = expectThrows(
                SearchPhaseExecutionException.class,
                prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(true)
            );

            assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
            assertThat(shardRestoringCause(e).recoveryId(), equalTo(restoreUuid));
        } finally {
            finishRestore();
        }
    }

    /**
     * With partial results allowed, a search over the restoring index and a healthy one answers from the healthy one and reports the
     * restoring shard as a failed shard, with its 403 status and type.
     */
    public void testPartialSearchReportsTheRestoringShardAsAFailure() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            final SearchResponse response = prepareSearch(RESTORED_INDEX, HEALTHY_INDEX).setAllowPartialSearchResults(true).get();
            try {
                assertThat(response.getFailedShards(), equalTo(1));
                assertThat(response.getSuccessfulShards(), equalTo(1));
                assertHitCount(response, DOCS);
                final var failure = response.getShardFailures()[0];
                assertThat(failure.status(), equalTo(RestStatus.FORBIDDEN));
                assertThat(failure.getCause(), instanceOf(ShardRestoringException.class));
                assertThat(((ShardRestoringException) failure.getCause()).recoveryId(), equalTo(restoreUuid));
            } finally {
                response.decRef();
            }
        } finally {
            finishRestore();
        }
    }

    /**
     * A restore that did not ask for its shards to be reported leaves the search failing as it always did, with a 503.
     */
    public void testSearchFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        setUpSnapshotAndHealthyIndex();
        blockAndStartRestore(false);
        try {
            final var e = expectThrows(
                SearchPhaseExecutionException.class,
                prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(false)
            );

            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertThat(
                ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof ShardRestoringException).isPresent(),
                equalTo(false)
            );
            final var partial = expectThrows(
                SearchPhaseExecutionException.class,
                prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(true)
            );
            assertThat(partial.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertThat(
                ExceptionsHelper.unwrapCause(partial.shardFailures()[0].getCause()),
                instanceOf(NoShardAvailableActionException.class)
            );
        } finally {
            finishRestore();
        }
    }

    /**
     * {@code GET /{index}/_doc/{id}}, which serves {@code _source}, {@code HEAD} and {@code _mget} items as well, fails with a 403.
     */
    public void testGetFailsWithShardRestoring() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            assertForbiddenShardRestoring(expectThrows(Exception.class, client().prepareGet(RESTORED_INDEX, "id")), restoreUuid);
            assertForbiddenShardRestoring(
                expectThrows(Exception.class, client().prepareGet(RESTORED_INDEX, "id").setRealtime(false)),
                restoreUuid
            );
        } finally {
            finishRestore();
        }
    }

    /**
     * {@code _mget} keeps its 200 and reports the restoring shard as the failure of each item, with a 403.
     */
    public void testMultiGetReportsShardRestoringPerItem() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            final var response = client().prepareMultiGet().add(RESTORED_INDEX, "a").add(RESTORED_INDEX, "b").get();
            assertThat(response.getResponses().length, equalTo(2));
            for (var item : response.getResponses()) {
                assertTrue(item.isFailed());
                assertForbiddenShardRestoring(item.getFailure().getFailure(), restoreUuid);
            }
        } finally {
            finishRestore();
        }
    }

    public void testExplainFailsWithShardRestoring() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            assertForbiddenShardRestoring(
                expectThrows(Exception.class, client().prepareExplain(RESTORED_INDEX, "id").setQuery(QueryBuilders.matchAllQuery())),
                restoreUuid
            );
        } finally {
            finishRestore();
        }
    }

    /**
     * {@code _termvectors} used to fail with a 500 that wrapped the cause of the failure, so this also checks that the exception comes out
     * of its wrapper.
     */
    public void testTermVectorsFailWithShardRestoring() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            assertForbiddenShardRestoring(expectThrows(Exception.class, client().prepareTermVectors(RESTORED_INDEX, "id")), restoreUuid);
        } finally {
            finishRestore();
        }
    }

    /**
     * {@code _mtermvectors} keeps its 200 and reports the restoring shard as the failure of each item.
     */
    public void testMultiTermVectorsReportShardRestoringPerItem() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            final var response = client().multiTermVectors(new MultiTermVectorsRequest().add(RESTORED_INDEX, "a").add(RESTORED_INDEX, "b"))
                .actionGet();
            assertThat(response.getResponses().length, equalTo(2));
            for (var item : response.getResponses()) {
                assertTrue(item.isFailed());
                assertForbiddenShardRestoring(item.getFailure().getCause(), restoreUuid);
            }
        } finally {
            finishRestore();
        }
    }

    public void testAnalyzeWithIndexFailsWithShardRestoring() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            assertForbiddenShardRestoring(
                expectThrows(Exception.class, indicesAdmin().prepareAnalyze(RESTORED_INDEX, "some text").setAnalyzer("standard")),
                restoreUuid
            );
        } finally {
            finishRestore();
        }
    }

    /**
     * {@code GET /{index}/_mapping/field/{fields}} fails with a 403 rather than answering with an empty body.
     */
    public void testGetFieldMappingFailsWithShardRestoring() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            assertForbiddenShardRestoring(
                expectThrows(Exception.class, indicesAdmin().prepareGetFieldMappings(RESTORED_INDEX).setFields("*")),
                restoreUuid
            );
        } finally {
            finishRestore();
        }
    }

    /**
     * {@code _msearch} keeps its 200 and reports the restoring shard as the failure of the sub-search.
     */
    public void testMultiSearchReportsShardRestoringPerSearch() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            final var response = client().prepareMultiSearch()
                .add(prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(false))
                .add(prepareSearch(HEALTHY_INDEX))
                .get();
            try {
                assertTrue(response.getResponses()[0].isFailure());
                assertForbiddenShardRestoring(response.getResponses()[0].getFailure(), restoreUuid);
                assertFalse(response.getResponses()[1].isFailure());
            } finally {
                response.decRef();
            }
        } finally {
            finishRestore();
        }
    }

    /**
     * {@code _field_caps} over several indices answers for the healthy ones and reports the restoring one as a failure with a 403.
     */
    public void testFieldCapsOverSeveralIndicesReportsTheRestoringIndexAsAFailure() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            final var response = client().fieldCaps(new FieldCapabilitiesRequest().indices(RESTORED_INDEX, HEALTHY_INDEX).fields("*"))
                .actionGet();
            assertThat(response.getFailedIndicesCount(), equalTo(1));
            assertThat(response.getFailures().size(), equalTo(1));
            final var failure = response.getFailures().get(0);
            assertThat(failure.getIndices(), arrayContaining(RESTORED_INDEX));
            assertForbiddenShardRestoring(failure.getException(), restoreUuid);
        } finally {
            finishRestore();
        }
    }

    /**
     * Opening a point in time with an index filter runs the can-match phase first, which is where the missing shard is found.
     */
    public void testOpenPointInTimeWithIndexFilterFailsWithShardRestoring() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            final var request = new OpenPointInTimeRequest(RESTORED_INDEX);
            request.keepAlive(TimeValue.timeValueMinutes(1));
            request.allowPartialSearchResults(false);
            request.indexFilter(QueryBuilders.matchAllQuery());
            assertForbiddenShardRestoring(
                expectThrows(Exception.class, client().execute(TransportOpenPointInTimeAction.TYPE, request)),
                restoreUuid
            );
        } finally {
            finishRestore();
        }
    }

    /**
     * {@code search_shards} runs the can-match phase when its query could rule shards out, and with partial results disallowed that
     * phase rejects a shard with no copy to search, as a search does. This is the action a cross-cluster search calls on each remote
     * cluster, which runs it against its own cluster state, so the restoring shard is reported whether the request names a cluster alias
     * (as a cross-cluster search does) or not.
     */
    public void testSearchShardsWithQueryWithoutPartialResultsFailsWithShardRestoring() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            for (String clusterAlias : new String[] { null, "cluster_a" }) {
                final var request = new SearchShardsRequest(
                    new String[] { RESTORED_INDEX },
                    SearchRequest.DEFAULT_INDICES_OPTIONS,
                    QueryBuilders.termQuery("field", "value"),
                    null,
                    null,
                    false,
                    clusterAlias
                );
                final Exception e = expectThrows(Exception.class, client().execute(TransportSearchShardsAction.TYPE, request));
                assertForbiddenShardRestoring(e, restoreUuid);
            }
        } finally {
            finishRestore();
        }
    }

    /**
     * Once the restore has finished the same search succeeds, so nothing about the restore lingers.
     */
    public void testSearchSucceedsOnceTheRestoreHasFinished() throws Exception {
        setUpSnapshotAndHealthyIndex();
        blockAndStartRestore(true);
        expectThrows(SearchPhaseExecutionException.class, prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(false));

        finishRestore();

        assertHitCount(prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(false), DOCS);
    }
}
