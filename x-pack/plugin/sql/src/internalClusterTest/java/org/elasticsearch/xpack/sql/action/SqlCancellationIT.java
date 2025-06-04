/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SqlCancellationIT extends AbstractSqlBlockingIntegTestCase {

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    /**
     * Shutdown the executor so we don't leak threads into other test runs.
     */
    @After
    public void shutdownExec() {
        executorService.shutdown();
    }

    public void testCancellation() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test").setMapping("val", "type=integer", "event_type", "type=keyword", "@timestamp", "type=date")
        );
        createIndex("idx_unmapped");

        int numDocs = randomIntBetween(6, 20);

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            int fieldValue = randomIntBetween(0, 10);
            builders.add(
                prepareIndex("test").setSource(
                    jsonBuilder().startObject()
                        .field("val", fieldValue)
                        .field("event_type", "my_event")
                        .field("@timestamp", "2020-04-09T12:35:48Z")
                        .endObject()
                )
            );
        }

        indexRandom(true, builders);
        boolean cancelDuringSearch = randomBoolean();
        List<SearchBlockPlugin> plugins = initBlockFactory(cancelDuringSearch, cancelDuringSearch == false);
        SqlQueryRequest request = new SqlQueryRequestBuilder(client()).query("SELECT event_type FROM test WHERE val=1").request();
        String id = randomAlphaOfLength(10);
        logger.trace("Preparing search");
        // We might perform field caps on the same thread if it is local client, so we cannot use the standard mechanism
        Future<SqlQueryResponse> future = executorService.submit(
            () -> client().filterWithHeader(Collections.singletonMap(Task.X_OPAQUE_ID_HTTP_HEADER, id))
                .execute(SqlQueryAction.INSTANCE, request)
                .get()
        );
        logger.trace("Waiting for block to be established");
        if (cancelDuringSearch) {
            awaitForBlockedSearches(plugins, "test");
        } else {
            awaitForBlockedFieldCaps(plugins);
        }
        logger.trace("Block is established");
        cancelTaskWithXOpaqueId(id, SqlQueryAction.NAME);

        disableBlocks(plugins);
        Exception exception = expectThrows(Exception.class, future::get);
        assertNotNull(ExceptionsHelper.unwrap(exception, TaskCancelledException.class));
        if (cancelDuringSearch) {
            // Make sure we cancelled inside search
            assertThat(getNumberOfContexts(plugins), greaterThan(0));
        } else {
            // Make sure we were not cancelled inside search
            assertThat(getNumberOfContexts(plugins), equalTo(0));
        }
    }
}
