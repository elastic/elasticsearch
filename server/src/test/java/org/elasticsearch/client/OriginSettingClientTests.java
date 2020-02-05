/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;

public class OriginSettingClientTests extends ESTestCase {
    public void testSetsParentId() {
        String origin = randomAlphaOfLength(7);

        /*
         * This mock will do nothing but verify that origin is set in the
         * thread context before executing the action.
         */
        NoOpClient mock = new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse>
                    void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
                assertEquals(origin, threadPool().getThreadContext().getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME));
                super.doExecute(action, request, listener);
            }
        };

        try (OriginSettingClient client = new OriginSettingClient(mock, origin)) {
            // All of these should have the origin set
            client.bulk(new BulkRequest());
            client.search(new SearchRequest());
            client.clearScroll(new ClearScrollRequest());

            ThreadContext threadContext = client.threadPool().getThreadContext();
            client.bulk(new BulkRequest(), listenerThatAssertsOriginNotSet(threadContext));
            client.search(new SearchRequest(), listenerThatAssertsOriginNotSet(threadContext));
            client.clearScroll(new ClearScrollRequest(), listenerThatAssertsOriginNotSet(threadContext));
        }
    }

    private <T> ActionListener<T> listenerThatAssertsOriginNotSet(ThreadContext threadContext) {
        return ActionListener.wrap(
                r -> {
                    assertNull(threadContext.getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME));
                },
                e -> {
                    fail("didn't expect to fail but: " + e);
                });
    }
}
