/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Single-shard reads against an index whose primary is being restored from a snapshot, on a stateless cluster. A restore that asks for its
 * shards to be reported makes them fail with a 403; one that does not leaves them failing with the 503 they always did.
 */
public class StatelessRestoringShardIT extends AbstractStatelessRestoringShardIT {

    private static final String PAINLESS_EXECUTE = """
        {
          "script": { "source": "true" },
          "context": "filter",
          "context_setup": { "index": "%s", "document": { "field": "value" } }
        }""".formatted(RESTORED_INDEX);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(PainlessPlugin.class);
        return plugins;
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
            assertServiceUnavailableException(expectThrows(Exception.class, client().prepareGet(RESTORED_INDEX, "id")));
            assertServiceUnavailableException(expectThrows(Exception.class, client().prepareGet(RESTORED_INDEX, "id").setRealtime(false)));
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
                assertServiceUnavailableException(item.getFailure().getFailure());
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
            assertServiceUnavailableException(
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
            assertServiceUnavailableException(expectThrows(Exception.class, client().prepareTermVectors(RESTORED_INDEX, "id")));
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
            assertServiceUnavailableException(
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
        setUpSnapshot(2, false);
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

            assertServiceUnavailableException(
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
                assertServiceUnavailableException(item.getFailure().getCause());
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
            assertServiceUnavailableException(
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
}
