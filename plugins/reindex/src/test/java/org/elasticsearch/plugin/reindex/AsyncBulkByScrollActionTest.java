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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.equalTo;

public class AsyncBulkByScrollActionTest extends ESTestCase {
    public void testThreadPoolRejections() throws Exception {
        ThreadPool rejectingThreadPool = new ThreadPool("test") {
            @Override
            public Executor generic() {
                return new Executor() {
                    @Override
                    public void execute(Runnable command) {
                        ((AbstractRunnable) command).onRejection(new EsRejectedExecutionException("test"));
                    }
                };
            }
        };
        try {
            SearchRequest firstSearchRequest = new SearchRequest();
            PlainActionFuture<Object> listener = new PlainActionFuture<Object>();
            new DummyAbstractAsyncBulkByScrollAction(null, rejectingThreadPool, null, firstSearchRequest, listener)
                    .onScrollResponse(new SearchResponse());
            try {
                listener.get();
                fail("Expected a failure");
            } catch (ExecutionException e) {
                assertThat(e.getMessage(), equalTo("EsRejectedExecutionException[test]"));
            }
        } finally {
            rejectingThreadPool.shutdown();
        }
    }


    private class DummyAbstractAsyncBulkByScrollAction extends AbstractAsyncBulkByScrollAction<DummyAbstractBulkByScrollRequest, Object> {
        public DummyAbstractAsyncBulkByScrollAction(Client client, ThreadPool threadPool,
                DummyAbstractBulkByScrollRequest mainRequest, SearchRequest firstSearchRequest, ActionListener<Object> listener) {
            super(logger, client, threadPool, mainRequest, firstSearchRequest, listener);
        }

        @Override
        protected BulkRequest buildBulk(Iterable<SearchHit> docs) {
            return new BulkRequest();
        }

        @Override
        protected Object buildResponse(long took) {
            return new Object();
        }
    }

    private static class DummyAbstractBulkByScrollRequest extends AbstractBulkByScrollRequest<DummyAbstractBulkByScrollRequest> {
        @Override
        protected DummyAbstractBulkByScrollRequest self() {
            return this;
        }
    }
}
