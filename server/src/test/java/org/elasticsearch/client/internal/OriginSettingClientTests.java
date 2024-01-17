/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;

public class OriginSettingClientTests extends ESTestCase {
    public void testSetsParentId() {
        String origin = randomAlphaOfLength(7);

        try (var threadPool = createThreadPool()) {
            /*
             * This mock will do nothing but verify that origin is set in the
             * thread context before executing the action.
             */
            final var mock = new NoOpClient(threadPool) {
                @Override
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    assertEquals(origin, threadPool().getThreadContext().getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME));
                }
            };

            final var client = new OriginSettingClient(mock, origin);
            // All of these should have the origin set
            BulkRequest bulkRequest1 = new BulkRequest();
            try {
                client.bulk(bulkRequest1);
            } finally {
                bulkRequest1.decRef();
            }
            client.search(new SearchRequest());
            client.clearScroll(new ClearScrollRequest());

            ThreadContext threadContext = client.threadPool().getThreadContext();
            BulkRequest bulkRequest2 = new BulkRequest();
            client.bulk(bulkRequest2, ActionListener.runAfter(listenerThatAssertsOriginNotSet(threadContext), bulkRequest2::decRef));
            client.search(new SearchRequest(), listenerThatAssertsOriginNotSet(threadContext));
            client.clearScroll(new ClearScrollRequest(), listenerThatAssertsOriginNotSet(threadContext));
        }
    }

    private <T> ActionListener<T> listenerThatAssertsOriginNotSet(ThreadContext threadContext) {
        return ActionTestUtils.assertNoFailureListener(
            r -> assertNull(threadContext.getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME))
        );
    }
}
