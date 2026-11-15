/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ObjectPath;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * The write APIs on a stateless cluster while their index is being restored from a snapshot. A restore that asks for its shards to be
 * reported makes them fail with a 403 straight away; one that does not leaves them failing with a 503 once their timeout expires.
 *
 * <p>These mirror the stateful tests in {@code RestoringShardIT}. The feature is only used on stateless, where the write goes to an index
 * node, a refresh also reaches the search shards and a flush uploads a commit, so what the stateful tests show for each API is not
 * assumed to hold here.
 */
public class StatelessRestoringShardWritesIT extends AbstractStatelessRestoringShardIT {

    /**
     * The timeout of a request that must fail without waiting for it, long enough for the test to fail instead of pass if it waits.
     */
    private static final TimeValue DOES_NOT_WAIT = TEST_REQUEST_TIMEOUT;
    /**
     * The timeout of a request that does wait for it, short so that the test does not.
     */
    private static final TimeValue WAITS = TimeValue.timeValueMillis(100);

    // index

    public void testIndexDocFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            final var e = expectThrows(
                Exception.class,
                client().prepareIndex(RESTORED_INDEX).setSource("f", "v").setTimeout(DOES_NOT_WAIT)
            );
            assertShardRestoringException(e, restoreUuid);
        });
    }

    public void testIndexDocFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            final var e = expectThrows(Exception.class, client().prepareIndex(RESTORED_INDEX).setSource("f", "v").setTimeout(WAITS));
            assertServiceUnavailableException(e);
        });
    }

    // create

    public void testCreateDocFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            final var e = expectThrows(Exception.class, createDoc(DOES_NOT_WAIT));
            assertShardRestoringException(e, restoreUuid);
        });
    }

    public void testCreateDocFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            final var e = expectThrows(Exception.class, createDoc(WAITS));
            assertServiceUnavailableException(e);
        });
    }

    private static IndexRequestBuilder createDoc(TimeValue timeout) {
        return client().prepareIndex(RESTORED_INDEX)
            .setId("new-create-id")
            .setSource("f", "v")
            .setOpType(DocWriteRequest.OpType.CREATE)
            .setTimeout(timeout);
    }

    // delete

    public void testDeleteDocFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            final var e = expectThrows(Exception.class, client().prepareDelete(RESTORED_INDEX, "some-id").setTimeout(DOES_NOT_WAIT));
            assertShardRestoringException(e, restoreUuid);
        });
    }

    public void testDeleteDocFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            final var e = expectThrows(Exception.class, client().prepareDelete(RESTORED_INDEX, "some-id").setTimeout(WAITS));
            assertServiceUnavailableException(e);
        });
    }

    // update

    /**
     * An update waits for its timeout either way, and then fails with the restore if it is reported.
     */
    public void testUpdateDocFailsWithShardRestoringAfterItsTimeout() throws Exception {
        whileRestoring(true, restoreUuid -> {
            final var e = expectThrows(
                Exception.class,
                client().prepareUpdate(RESTORED_INDEX, "some-id").setDoc("f", "v").setTimeout(WAITS)
            );
            assertShardRestoringException(e, restoreUuid);
        });
    }

    public void testUpdateDocFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            final var e = expectThrows(
                Exception.class,
                client().prepareUpdate(RESTORED_INDEX, "some-id").setDoc("f", "v").setTimeout(WAITS)
            );
            assertServiceUnavailableException(e);
        });
    }

    // bulk

    /**
     * A bulk keeps its 200 envelope and fails each item.
     */
    public void testBulkFailsEachItemWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            final BulkResponse bulk = bulk(DOES_NOT_WAIT);
            assertTrue(bulk.hasFailures());
            for (BulkItemResponse item : bulk) {
                assertTrue(item.isFailed());
                assertThat(item.getFailure().getStatus(), equalTo(RestStatus.FORBIDDEN));
                assertShardRestoringException(item.getFailure().getCause(), restoreUuid);
            }
        });
    }

    public void testBulkFailsEachItemWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            final BulkResponse bulk = bulk(WAITS);
            assertTrue(bulk.hasFailures());
            for (BulkItemResponse item : bulk) {
                assertTrue(item.isFailed());
                assertThat(item.getFailure().getStatus(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
                assertServiceUnavailableException(item.getFailure().getCause());
            }
        });
    }

    private static BulkResponse bulk(TimeValue timeout) {
        return client().prepareBulk()
            .add(client().prepareIndex(RESTORED_INDEX).setId("new1").setSource("f", "v"))
            .add(client().prepareIndex(RESTORED_INDEX).setId("new2").setSource("f", "v").setOpType(DocWriteRequest.OpType.CREATE))
            .add(client().prepareDelete(RESTORED_INDEX, "some-id"))
            .setTimeout(timeout)
            .get();
    }

    /**
     * The serialized form clients parse.
     */
    public void testBulkOverRestFailsEachItemWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            final ObjectPath bulk = performExpectingOk(bulkRequest());
            assertThat(bulk.evaluate("items.0.index.status"), equalTo(RestStatus.FORBIDDEN.getStatus()));
            assertThat(bulk.evaluate("items.0.index.error.type"), equalTo("shard_restoring_exception"));
        });
    }

    public void testBulkOverRestFailsEachItemWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            final ObjectPath bulk = performExpectingOk(bulkRequest());
            assertThat(bulk.evaluate("items.0.index.status"), equalTo(RestStatus.SERVICE_UNAVAILABLE.getStatus()));
        });
    }

    private static Request bulkRequest() {
        final Request request = new Request("POST", "/_bulk");
        request.addParameter("timeout", "100ms");
        request.setJsonEntity("{\"index\": {\"_index\": \"" + RESTORED_INDEX + "\"}}\n{\"f\": \"v\"}\n");
        return request;
    }

    // add block

    /**
     * Adding a block is not acknowledged when a shard fails its verification, and the failed call is not rolled back.
     */
    public void testAddBlockReportsShardRestoringShardFailure() throws Exception {
        whileRestoring(true, restoreUuid -> {
            final var response = indicesAdmin().prepareAddBlock(IndexMetadata.APIBlock.WRITE, RESTORED_INDEX)
                .setTimeout(DOES_NOT_WAIT)
                .get();
            assertFalse(response.isAcknowledged());
            final var failure = response.getIndices().getFirst().getShards()[0].getFailures()[0];
            assertThat(failure.status(), equalTo(RestStatus.FORBIDDEN));
            assertShardRestoringException(failure.getCause(), restoreUuid);
        });
    }

    public void testAddBlockReportsServiceUnavailableShardFailureWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            final var response = indicesAdmin().prepareAddBlock(IndexMetadata.APIBlock.WRITE, RESTORED_INDEX).setTimeout(WAITS).get();
            assertFalse(response.isAcknowledged());
            final var failure = response.getIndices().getFirst().getShards()[0].getFailures()[0];
            assertThat(failure.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertServiceUnavailableException(failure.getCause());
        });
    }

    // close

    public void testCloseIsRejectedWithTheRestoreReportingItsShards() throws Exception {
        whileRestoring(true, restoreUuid -> assertCloseIsRejected());
    }

    public void testCloseIsRejectedWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> assertCloseIsRejected());
    }

    private static void assertCloseIsRejected() {
        final var e = expectThrows(IllegalArgumentException.class, indicesAdmin().prepareClose(RESTORED_INDEX));
        assertThat(e.getMessage(), containsString("Cannot close indices that are being restored"));
    }

    // refresh, flush and force merge: the stateful cluster swallows the exception and reports no failed shard. Stateless may differ.

    public void testRefreshWithTheRestoreReportingItsShards() throws Exception {
        whileRestoring(true, restoreUuid -> assertThat(indicesAdmin().prepareRefresh(RESTORED_INDEX).get().getFailedShards(), equalTo(0)));
    }

    public void testRefreshWhenTheRestoreDoesNotReportItsShards() throws Exception {
        // without the flag the refresh waits for the primary until its timeout, which is a minute by default, before swallowing the failure
        whileRestoring(
            false,
            restoreUuid -> assertThat(indicesAdmin().prepareRefresh(RESTORED_INDEX).setTimeout(WAITS).get().getFailedShards(), equalTo(0))
        );
    }

    public void testFlushWithTheRestoreReportingItsShards() throws Exception {
        whileRestoring(true, restoreUuid -> assertThat(indicesAdmin().prepareFlush(RESTORED_INDEX).get().getFailedShards(), equalTo(0)));
    }

    public void testFlushWhenTheRestoreDoesNotReportItsShards() throws Exception {
        // like a refresh, a flush without the flag waits for the primary until its timeout, a minute by default
        whileRestoring(
            false,
            restoreUuid -> assertThat(indicesAdmin().prepareFlush(RESTORED_INDEX).setTimeout(WAITS).get().getFailedShards(), equalTo(0))
        );
    }

    public void testForceMergeWithTheRestoreReportingItsShards() throws Exception {
        whileRestoring(
            true,
            restoreUuid -> assertThat(indicesAdmin().prepareForceMerge(RESTORED_INDEX).get().getFailedShards(), equalTo(0))
        );
    }

    public void testForceMergeWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(
            false,
            restoreUuid -> assertThat(indicesAdmin().prepareForceMerge(RESTORED_INDEX).get().getFailedShards(), equalTo(0))
        );
    }
}
