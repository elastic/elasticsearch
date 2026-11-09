/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.ShardRestoringException;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.unblockAllDataNodes;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Single-shard reads against an index whose primary is being restored from a snapshot, on a stateless cluster. A restore that asks for its
 * shards to be reported makes them fail with a 403; one that does not leaves them failing with the 503 they always did.
 *
 * <p>In stateless a restoring primary is on an index node, which is not searchable, and its search copy is only assigned once the
 * primary is active. There is therefore no copy of the restoring shard to send a read to, so each of these fails on the coordinator
 * without asking any node. A stateful cluster sends the read to the initializing primary instead.
 */
public class StatelessRestoringShardIT extends AbstractStatelessPluginIntegTestCase {

    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";
    private static final String RESTORED_INDEX = "restored-index";

    private static final String PAINLESS_EXECUTE = Strings.format("""
        {
          "script": { "source": "true" },
          "context": "filter",
          "context_setup": { "index": "%s", "document": { "field": "value" } }
        }""", RESTORED_INDEX);

    /**
     * The REST tests need a real HTTP transport.
     */
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    /**
     * The base class registers its own repository type named {@code mock}, an object store backed by the file system, which cannot block.
     * The snapshot repository here has to, to hold the restore in flight, so it takes that name instead.
     */
    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockRepository.Plugin.class);
        plugins.add(PainlessPlugin.class);
        return plugins;
    }

    /**
     * Creates a single-shard index with documents, snapshots it and deletes it.
     */
    private void setUpSnapshot() {
        setUpSnapshot(1);
    }

    /**
     * Like {@link #setUpSnapshot()}, with {@link #RESTORED_INDEX} split into {@code shards} shards.
     *
     * <p>Every shard gets documents: an empty shard reads nothing from the repository, so its restore would not wait on the block and
     * would finish straight away.
     */
    private void setUpSnapshot(int shards) {
        startMasterOnlyNode();
        startIndexNodes(1);
        startSearchNode();
        createIndex(RESTORED_INDEX, indexSettings(shards, 1).build());
        ensureGreen(RESTORED_INDEX);
        indexDocs(RESTORED_INDEX, 100 * shards);
        flush(RESTORED_INDEX);
        for (ShardStats shardStats : indicesAdmin().prepareStats(RESTORED_INDEX).clear().setDocs(true).get().getShards()) {
            if (shardStats.getShardRouting().primary()) {
                assertThat(shardStats.getShardRouting().toString(), shardStats.getStats().getDocs().getCount(), greaterThan(0L));
            }
        }
        createRepository(REPO, "mock");
        createSnapshot(REPO, SNAPSHOT, List.of(RESTORED_INDEX), List.of());
        assertAcked(indicesAdmin().prepareDelete(RESTORED_INDEX));
    }

    /**
     * Blocks the repository, starts a restore of {@link #RESTORED_INDEX} and waits until every one of its primaries is initializing from
     * the snapshot.
     *
     * @return the restore UUID, which the exception carries as its recovery ID
     */
    private String blockAndStartRestore(boolean reportShardRestoring) {
        final String restoreUuid = AbstractSnapshotIntegTestCase.blockAndStartRestore(REPO, SNAPSHOT, RESTORED_INDEX, reportShardRestoring);
        awaitClusterState(state -> {
            final IndexRoutingTable indexRouting = state.routingTable().index(RESTORED_INDEX);
            for (int shard = 0; shard < indexRouting.size(); shard++) {
                final ShardRouting primary = indexRouting.shard(shard).primaryShard();
                if (primary.state() != ShardRoutingState.INITIALIZING
                    || primary.recoverySource().getType() != RecoverySource.Type.SNAPSHOT) {
                    return false;
                }
            }
            return true;
        });
        return restoreUuid;
    }

    private void finishRestore() throws Exception {
        unblockAllDataNodes(REPO);
        ensureGreen(RESTORED_INDEX);
    }

    /**
     * Runs {@code body} while {@link #RESTORED_INDEX} is being restored by a restore that does or does not report its shards, passing it
     * the restore UUID.
     */
    private void whileRestoring(boolean reportShardRestoring, CheckedConsumer<String, Exception> body) throws Exception {
        setUpSnapshot();
        final String restoreUuid = blockAndStartRestore(reportShardRestoring);
        try {
            body.accept(restoreUuid);
        } finally {
            finishRestore();
        }
    }

    // get

    /**
     * {@code GET /{index}/_doc/{id}} fails with a 403. {@code HEAD /{index}/_doc/{id}} and {@code GET|HEAD /{index}/_source/{id}} run
     * the same transport action, so they do too.
     */
    public void testGetFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            assertShardRestoringException(expectThrows(Exception.class, client().prepareGet(RESTORED_INDEX, "id")), restoreUuid);
            assertShardRestoringException(
                expectThrows(Exception.class, client().prepareGet(RESTORED_INDEX, "id").setRealtime(false)),
                restoreUuid
            );
        });
    }

    public void testGetFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            assertServiceUnavailableWithoutShardRestoring(expectThrows(Exception.class, client().prepareGet(RESTORED_INDEX, "id")));
            assertServiceUnavailableWithoutShardRestoring(
                expectThrows(Exception.class, client().prepareGet(RESTORED_INDEX, "id").setRealtime(false))
            );
        });
    }

    /**
     * Once the restore has finished the same get is served, so nothing about the restore lingers. The id is not one of the indexed
     * documents, so the get finds nothing: what matters is that a copy answers it.
     */
    public void testGetIsServedOnceTheRestoreHasFinished() throws Exception {
        setUpSnapshot();
        final String restoreUuid = blockAndStartRestore(true);
        assertShardRestoringException(expectThrows(Exception.class, client().prepareGet(RESTORED_INDEX, "id")), restoreUuid);

        finishRestore();

        assertFalse(client().prepareGet(RESTORED_INDEX, "id").get().isExists());
    }

    /**
     * What a client gets over HTTP from {@code GET|HEAD /{index}/_doc/{id}} and {@code GET|HEAD /{index}/_source/{id}}: a 403 with the
     * restore in the error body, which a HEAD does not have.
     */
    public void testDocReadsOverRestFailWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            for (String path : List.of(
                "/" + RESTORED_INDEX + "/_doc/id",
                "/" + RESTORED_INDEX + "/_doc/id?realtime=false",
                "/" + RESTORED_INDEX + "/_source/id"
            )) {
                final ObjectPath error = performExpectingError(new Request("GET", path), 403);
                assertThat(path, error.evaluate("error.type"), equalTo("shard_restoring_exception"));
                assertThat(path, error.evaluate("error.index"), equalTo(RESTORED_INDEX));
                assertThat(path, error.evaluate("error.recovery_id"), equalTo(restoreUuid));
                assertThat(path, error.evaluate("status"), equalTo(403));
            }
            for (String path : List.of("/" + RESTORED_INDEX + "/_doc/id", "/" + RESTORED_INDEX + "/_source/id")) {
                assertNull(performExpectingError(new Request("HEAD", path), 403));
            }
        });
    }

    public void testDocReadsOverRestFailWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            for (String path : List.of("/" + RESTORED_INDEX + "/_doc/id", "/" + RESTORED_INDEX + "/_source/id")) {
                final ObjectPath error = performExpectingError(new Request("GET", path), 503);
                assertThat(path, error.evaluate("error.type"), equalTo("no_shard_available_action_exception"));
                assertNull(performExpectingError(new Request("HEAD", path), 503));
            }
        });
    }

    // mget

    /**
     * {@code _mget} succeeds and reports the restoring shard as the failure of each item, with a 403.
     */
    public void testMultiGetReportsShardRestoringPerItem() throws Exception {
        whileRestoring(true, restoreUuid -> {
            final var response = client().prepareMultiGet().add(RESTORED_INDEX, "a").add(RESTORED_INDEX, "b").get();
            assertThat(response.getResponses().length, equalTo(2));
            for (var item : response.getResponses()) {
                assertTrue(item.isFailed());
                assertShardRestoringException(item.getFailure().getFailure(), restoreUuid);
            }
        });
    }

    public void testMultiGetReportsServiceUnavailablePerItemWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            final var response = client().prepareMultiGet().add(RESTORED_INDEX, "a").add(RESTORED_INDEX, "b").get();
            assertThat(response.getResponses().length, equalTo(2));
            for (var item : response.getResponses()) {
                assertTrue(item.isFailed());
                assertServiceUnavailableWithoutShardRestoring(item.getFailure().getFailure());
            }
        });
    }

    // explain

    public void testExplainFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            assertShardRestoringException(
                expectThrows(Exception.class, client().prepareExplain(RESTORED_INDEX, "id").setQuery(QueryBuilders.matchAllQuery())),
                restoreUuid
            );
        });
    }

    public void testExplainFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            assertServiceUnavailableWithoutShardRestoring(
                expectThrows(Exception.class, client().prepareExplain(RESTORED_INDEX, "id").setQuery(QueryBuilders.matchAllQuery()))
            );
        });
    }

    // term vectors

    /**
     * {@code _termvectors} has no copy to send the request to, so it fails on the coordinator with a 403, rather than with the 500 that
     * the initializing primary of a stateful cluster answers with.
     */
    public void testTermVectorsFailWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            assertShardRestoringException(expectThrows(Exception.class, client().prepareTermVectors(RESTORED_INDEX, "id")), restoreUuid);
        });
    }

    /**
     * With no copy to send the request to, {@code _termvectors} fails with a 503 here, where a stateful cluster answers with a 500.
     */
    public void testTermVectorsFailWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            assertServiceUnavailableWithoutShardRestoring(expectThrows(Exception.class, client().prepareTermVectors(RESTORED_INDEX, "id")));
        });
    }

    /**
     * {@code _termvectors} for an artificial document, which has no id to route by, picks a shard of the index as a search would, and
     * fails with a 403 too.
     */
    public void testTermVectorsForAnArtificialDocumentFailWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            assertShardRestoringException(
                expectThrows(Exception.class, client().prepareTermVectors().setIndex(RESTORED_INDEX).setDoc(artificialDocument())),
                restoreUuid
            );
        });
    }

    public void testTermVectorsForAnArtificialDocumentFailWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            assertServiceUnavailableWithoutShardRestoring(
                expectThrows(Exception.class, client().prepareTermVectors().setIndex(RESTORED_INDEX).setDoc(artificialDocument()))
            );
        });
    }

    private static XContentBuilder artificialDocument() throws IOException {
        return XContentFactory.jsonBuilder().startObject().field("field", "some text").endObject();
    }

    /**
     * A {@code _shards} preference that leaves out the shard a document routes to means no copy could ever serve the request, so it is
     * not the restore that stops it: it keeps its 503. The same request with a preference for that shard is a 403.
     */
    public void testTermVectorsWithAPreferenceThatLeavesOutTheRoutedShardFailWithServiceUnavailable() throws Exception {
        setUpSnapshot(2);
        final String restoreUuid = blockAndStartRestore(true);
        try {
            // the master is the node that blockAndStartRestore waited on, so it is known to have the restored index
            final var indexMetadata = internalCluster().clusterService(internalCluster().getMasterName())
                .state()
                .metadata()
                .getProject(ProjectId.DEFAULT)
                .index(RESTORED_INDEX);
            final int routedShard = IndexRouting.fromIndexMetadata(indexMetadata).getShard("id", null);
            final int otherShard = 1 - routedShard;

            assertServiceUnavailableWithoutShardRestoring(
                expectThrows(Exception.class, client().prepareTermVectors(RESTORED_INDEX, "id").setPreference("_shards:" + otherShard))
            );
            assertShardRestoringException(
                expectThrows(Exception.class, client().prepareTermVectors(RESTORED_INDEX, "id").setPreference("_shards:" + routedShard)),
                restoreUuid
            );
        } finally {
            finishRestore();
        }
    }

    // mtermvectors

    /**
     * {@code _mtermvectors} succeeds and reports the restoring shard as the failure of each item, with a 403.
     */
    public void testMultiTermVectorsReportShardRestoringPerItem() throws Exception {
        whileRestoring(true, restoreUuid -> {
            final var response = client().multiTermVectors(new MultiTermVectorsRequest().add(RESTORED_INDEX, "a").add(RESTORED_INDEX, "b"))
                .actionGet();
            assertThat(response.getResponses().length, equalTo(2));
            for (var item : response.getResponses()) {
                assertTrue(item.isFailed());
                assertShardRestoringException(item.getFailure().getCause(), restoreUuid);
            }
        });
    }

    public void testMultiTermVectorsReportServiceUnavailablePerItemWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            final var response = client().multiTermVectors(new MultiTermVectorsRequest().add(RESTORED_INDEX, "a").add(RESTORED_INDEX, "b"))
                .actionGet();
            assertThat(response.getResponses().length, equalTo(2));
            for (var item : response.getResponses()) {
                assertTrue(item.isFailed());
                assertServiceUnavailableWithoutShardRestoring(item.getFailure().getCause());
            }
        });
    }

    // analyze, which reads from any shard of the index

    public void testAnalyzeWithIndexFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            assertShardRestoringException(
                expectThrows(Exception.class, indicesAdmin().prepareAnalyze(RESTORED_INDEX, "some text").setAnalyzer("standard")),
                restoreUuid
            );
        });
    }

    public void testAnalyzeWithIndexFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            assertServiceUnavailableWithoutShardRestoring(
                expectThrows(Exception.class, indicesAdmin().prepareAnalyze(RESTORED_INDEX, "some text").setAnalyzer("standard"))
            );
        });
    }

    // painless execute, which reads from any shard of the index

    /**
     * {@code POST /_scripts/painless/_execute} with an index in its {@code context_setup}, which makes it run on a shard of that index,
     * fails with a 403.
     */
    public void testPainlessExecuteWithIndexContextFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            final Request request = new Request("POST", "/_scripts/painless/_execute");
            request.setJsonEntity(PAINLESS_EXECUTE);
            final ObjectPath error = performExpectingError(request, 403);
            assertThat(error.evaluate("error.type"), equalTo("shard_restoring_exception"));
            assertThat(error.evaluate("error.recovery_id"), equalTo(restoreUuid));
        });
    }

    public void testPainlessExecuteWithIndexContextFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            final Request request = new Request("POST", "/_scripts/painless/_execute");
            request.setJsonEntity(PAINLESS_EXECUTE);
            final ObjectPath error = performExpectingError(request, 503);
            assertThat(error.evaluate("error.type"), equalTo("no_shard_available_action_exception"));
        });
    }

    /**
     * Performs a request that is expected to fail with {@code status} and returns its error body, or {@code null} for a HEAD, which has
     * none.
     */
    private static ObjectPath performExpectingError(Request request, int status) throws IOException {
        final ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(request.getMethod() + " " + request.getEndpoint(), e.getResponse().getStatusLine().getStatusCode(), equalTo(status));
        if (request.getMethod().equals("HEAD")) {
            return null;
        }
        return ObjectPath.createFromResponse(e.getResponse());
    }

    private static void assertShardRestoringException(Throwable e, String restoreUuid) {
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.FORBIDDEN));
        final var restoring = ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof ShardRestoringException);
        assertTrue("no ShardRestoringException in the causes of " + e, restoring.isPresent());
        assertThat(((ShardRestoringException) restoring.get()).recoveryId(), equalTo(restoreUuid));
    }

    /**
     * Asserts the failure a restoring shard has always produced: a 503 with no {@link ShardRestoringException} among its causes.
     */
    private static void assertServiceUnavailableWithoutShardRestoring(Throwable e) {
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(NoShardAvailableActionException.class));
        assertFalse(
            "unexpected ShardRestoringException in the causes of " + e,
            ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof ShardRestoringException).isPresent()
        );
    }
}
