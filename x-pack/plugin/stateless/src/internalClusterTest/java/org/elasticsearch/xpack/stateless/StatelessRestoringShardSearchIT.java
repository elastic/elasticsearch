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
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ObjectPath;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Searches, and the APIs that run like one, against an index whose primary is being restored from a snapshot, on a stateless cluster. A
 * restore that asks for its shards to be reported makes a search that cannot proceed fail with a 403, and a partial search report the
 * restoring shard as a 403 failure; one that does not leaves them as they always were, with a 503.
 */
public class StatelessRestoringShardSearchIT extends AbstractStatelessRestoringShardIT {

    // _search

    /**
     * With partial results disallowed, a search over the restoring index is rejected up front with a 403 that says which restore it is
     * waiting for.
     */
    public void testSearchWithoutPartialResultsFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            assertShardRestoringException(
                expectThrows(SearchPhaseExecutionException.class, prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(false)),
                restoreUuid
            );
        });
    }

    public void testSearchWithoutPartialResultsFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            assertServiceUnavailableException(
                expectThrows(SearchPhaseExecutionException.class, prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(false))
            );
        });
    }

    /**
     * With partial results allowed, searching only the restoring index fails because no shard can answer, and the failure is the
     * restoring one.
     */
    public void testSearchOfOnlyTheRestoringIndexFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            assertShardRestoringException(
                expectThrows(SearchPhaseExecutionException.class, prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(true)),
                restoreUuid
            );
        });
    }

    public void testSearchOfOnlyTheRestoringIndexFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            final var e = expectThrows(
                SearchPhaseExecutionException.class,
                prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(true)
            );
            assertServiceUnavailableException(e);
            assertThat(ExceptionsHelper.unwrapCause(e.shardFailures()[0].getCause()), instanceOf(NoShardAvailableActionException.class));
        });
    }

    /**
     * With partial results allowed, a search over the restoring index and a healthy one answers from the healthy one and reports the
     * restoring shard as a failed shard, with its 403 status and type.
     */
    public void testPartialSearchReportsTheRestoringShardAsAFailure() throws Exception {
        whileRestoring(true, true, restoreUuid -> {
            assertResponse(prepareSearch(RESTORED_INDEX, HEALTHY_INDEX).setAllowPartialSearchResults(true), response -> {
                assertThat(response.getFailedShards(), equalTo(1));
                assertThat(response.getSuccessfulShards(), equalTo(1));
                assertHitCount(response, 100);
                final var failure = response.getShardFailures()[0];
                assertThat(failure.status(), equalTo(RestStatus.FORBIDDEN));
                assertShardRestoringException(failure.getCause(), restoreUuid);
            });
        });
    }

    public void testPartialSearchReportsTheRestoringShardAsServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(true, false, restoreUuid -> {
            assertResponse(prepareSearch(RESTORED_INDEX, HEALTHY_INDEX).setAllowPartialSearchResults(true), response -> {
                assertThat(response.getFailedShards(), equalTo(1));
                assertThat(response.getSuccessfulShards(), equalTo(1));
                assertHitCount(response, 100);
                final var failure = response.getShardFailures()[0];
                assertThat(failure.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
                assertThat(ExceptionsHelper.unwrapCause(failure.getCause()), instanceOf(NoShardAvailableActionException.class));
            });
        });
    }

    /**
     * Once the restore has finished the same search succeeds, so nothing about the restore lingers.
     */
    public void testSearchSucceedsOnceTheRestoreHasFinished() throws Exception {
        setUpSnapshot();
        blockAndStartRestore(true);
        expectThrows(SearchPhaseExecutionException.class, prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(false));

        finishRestore();

        assertHitCount(prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(false), 100);
    }

    /**
     * What a client gets over HTTP from {@code POST /{index}/_search}, {@code POST /{index}/_count} and {@code GET /_cat/count/{index}}:
     * a 403 whose error carries the restore. With partial results disallowed the search is rejected before any shard is asked, so the
     * restore is the cause of the rejection and there is no root cause; otherwise every shard fails, and the restore is the root cause
     * and the shard failure.
     */
    public void testSearchAndCountOverRestFailWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            final String rejected = "/" + RESTORED_INDEX + "/_search?allow_partial_search_results=false";
            final ObjectPath rejection = performExpectingError(new Request("POST", rejected), 403);
            assertThat(rejection.evaluate("status"), equalTo(403));
            assertThat(rejection.evaluate("error.root_cause"), equalTo(List.of()));
            assertThat(rejection.evaluate("error.caused_by.caused_by.type"), equalTo("shard_restoring_exception"));
            assertThat(rejection.evaluate("error.caused_by.caused_by.recovery_id"), equalTo(restoreUuid));

            for (Request request : new Request[] {
                new Request("POST", "/" + RESTORED_INDEX + "/_search"),
                new Request("POST", "/" + RESTORED_INDEX + "/_count"),
                new Request("GET", "/_cat/count/" + RESTORED_INDEX + "?format=json") }) {
                final String path = request.getEndpoint();
                final ObjectPath error = performExpectingError(request, 403);
                assertThat(path, error.evaluate("status"), equalTo(403));
                assertThat(path, error.evaluate("error.root_cause.0.type"), equalTo("shard_restoring_exception"));
                assertThat(path, error.evaluate("error.root_cause.0.recovery_id"), equalTo(restoreUuid));
                assertThat(path, error.evaluate("error.failed_shards.0.reason.type"), equalTo("shard_restoring_exception"));
            }
        });
    }

    public void testSearchAndCountOverRestFailWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            for (Request request : new Request[] {
                new Request("POST", "/" + RESTORED_INDEX + "/_search?allow_partial_search_results=false"),
                new Request("POST", "/" + RESTORED_INDEX + "/_search"),
                new Request("POST", "/" + RESTORED_INDEX + "/_count"),
                new Request("GET", "/_cat/count/" + RESTORED_INDEX + "?format=json") }) {
                final ObjectPath error = performExpectingError(request, 503);
                assertThat(request.getEndpoint(), error.evaluate("status"), equalTo(503));
            }
        });
    }

    // _msearch

    public void testMultiSearchReportsShardRestoringPerSearch() throws Exception {
        whileRestoring(true, true, restoreUuid -> {
            assertResponse(
                client().prepareMultiSearch()
                    .add(prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(false))
                    .add(prepareSearch(HEALTHY_INDEX)),
                response -> {
                    assertTrue(response.getResponses()[0].isFailure());
                    assertShardRestoringException(response.getResponses()[0].getFailure(), restoreUuid);
                    assertFalse(response.getResponses()[1].isFailure());
                }
            );
        });
    }

    public void testMultiSearchReportsServiceUnavailablePerSearchWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(true, false, restoreUuid -> {
            assertResponse(
                client().prepareMultiSearch()
                    .add(prepareSearch(RESTORED_INDEX).setAllowPartialSearchResults(false))
                    .add(prepareSearch(HEALTHY_INDEX)),
                response -> {
                    assertTrue(response.getResponses()[0].isFailure());
                    assertServiceUnavailableException(response.getResponses()[0].getFailure());
                    assertFalse(response.getResponses()[1].isFailure());
                }
            );
        });
    }

    // point in time

    /**
     * Opening a point in time fails with a 403, whether it goes straight to the shards or runs the can-match phase first, as it does
     * with an index filter.
     */
    public void testOpenPointInTimeFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            for (boolean withIndexFilter : new boolean[] { false, true }) {
                assertShardRestoringException(
                    expectThrows(Exception.class, client().execute(TransportOpenPointInTimeAction.TYPE, openPointInTime(withIndexFilter))),
                    restoreUuid
                );
            }
        });
    }

    public void testOpenPointInTimeFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            for (boolean withIndexFilter : new boolean[] { false, true }) {
                assertServiceUnavailableException(
                    expectThrows(Exception.class, client().execute(TransportOpenPointInTimeAction.TYPE, openPointInTime(withIndexFilter)))
                );
            }
        });
    }

    private static OpenPointInTimeRequest openPointInTime(boolean withIndexFilter) {
        final var request = new OpenPointInTimeRequest(RESTORED_INDEX);
        request.keepAlive(TimeValue.timeValueMinutes(1));
        request.allowPartialSearchResults(false);
        if (withIndexFilter) {
            request.indexFilter(QueryBuilders.matchAllQuery());
        }
        return request;
    }

    // _field_caps

    /**
     * {@code _field_caps} over several indices answers for the healthy one and reports the restoring one as a failure with a 403.
     */
    public void testFieldCapsOverSeveralIndicesReportsTheRestoringIndexAsAFailure() throws Exception {
        whileRestoring(true, true, restoreUuid -> {
            final var response = client().fieldCaps(new FieldCapabilitiesRequest().indices(RESTORED_INDEX, HEALTHY_INDEX).fields("*"))
                .actionGet();
            assertThat(response.getFailedIndicesCount(), equalTo(1));
            assertThat(response.getFailures().size(), equalTo(1));
            final var failure = response.getFailures().get(0);
            assertThat(failure.getIndices(), arrayContaining(RESTORED_INDEX));
            assertShardRestoringException(failure.getException(), restoreUuid);

            final ObjectPath body = performExpectingOk(
                new Request("GET", "/" + RESTORED_INDEX + "," + HEALTHY_INDEX + "/_field_caps?fields=*")
            );
            assertThat(body.evaluate("failed_indices"), equalTo(1));
            assertThat(body.evaluate("failures.0.failure.error.type"), equalTo("shard_restoring_exception"));
            assertThat(body.evaluate("failures.0.failure.error.recovery_id"), equalTo(restoreUuid));
        });
    }

    public void testFieldCapsOverSeveralIndicesReportsServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(true, false, restoreUuid -> {
            final var response = client().fieldCaps(new FieldCapabilitiesRequest().indices(RESTORED_INDEX, HEALTHY_INDEX).fields("*"))
                .actionGet();
            assertThat(response.getFailedIndicesCount(), equalTo(1));
            final var failure = response.getFailures().get(0);
            assertThat(failure.getIndices(), arrayContaining(RESTORED_INDEX));
            assertServiceUnavailableException(failure.getException());
        });
    }

    /**
     * When no index answers, {@code _field_caps} fails with a single exception, which is the restore when it is all that stops the
     * request.
     */
    public void testFieldCapsOfOnlyTheRestoringIndexFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            assertShardRestoringException(
                expectThrows(Exception.class, client().fieldCaps(new FieldCapabilitiesRequest().indices(RESTORED_INDEX).fields("*"))),
                restoreUuid
            );

            final ObjectPath error = performExpectingError(new Request("GET", "/" + RESTORED_INDEX + "/_field_caps?fields=*"), 403);
            assertThat(error.evaluate("status"), equalTo(403));
            assertThat(error.evaluate("error.type"), equalTo("shard_restoring_exception"));
            assertThat(error.evaluate("error.recovery_id"), equalTo(restoreUuid));
        });
    }

    public void testFieldCapsOfOnlyTheRestoringIndexFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            assertServiceUnavailableException(
                expectThrows(Exception.class, client().fieldCaps(new FieldCapabilitiesRequest().indices(RESTORED_INDEX).fields("*")))
            );

            final ObjectPath error = performExpectingError(new Request("GET", "/" + RESTORED_INDEX + "/_field_caps?fields=*"), 503);
            assertThat(error.evaluate("status"), equalTo(503));
            assertThat(error.evaluate("error.type"), equalTo("no_shard_available_action_exception"));
        });
    }
}
