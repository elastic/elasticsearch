/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.test.ESIntegTestCase.client;
import static org.elasticsearch.test.ESIntegTestCase.indexExists;
import static org.elasticsearch.test.ESIntegTestCase.internalCluster;
import static org.elasticsearch.test.ESTestCase.TEST_REQUEST_TIMEOUT;
import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.XPackPlugin.ASYNC_RESULTS_INDEX;
import static org.junit.Assert.assertFalse;

/**
 * Test helpers for integration tests that store results in the async results index.
 */
public final class AsyncResultsTestUtils {

    private AsyncResultsTestUtils() {}

    /**
     * Waits for in-flight async tasks to store their final result, then deletes the async results index.
     * A late write after the index is deleted re-creates it, and the new shard's lock fails
     * InternalTestCluster#assertAfterTest. The index may be an alias (see AsyncSearchIndexAliasIT),
     * so it's resolved to a concrete name first.
     */
    public static void awaitAsyncTasksAndDeleteResultsIndex() throws Exception {
        assertBusy(() -> {
            for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                for (CancellableTask task : transportService.getTaskManager().getCancellableTasks().values()) {
                    assertFalse("async task still running: " + task.getDescription(), task instanceof AsyncTask);
                }
            }
        });
        if (indexExists(ASYNC_RESULTS_INDEX, client())) {
            String[] concreteIndices = client().admin()
                .indices()
                .prepareGetIndex(TEST_REQUEST_TIMEOUT)
                .setIndices(ASYNC_RESULTS_INDEX)
                .get()
                .getIndices();
            assertAcked(client().admin().indices().prepareDelete(concreteIndices));
        }
    }
}
