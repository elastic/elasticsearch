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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;

public class AsyncBulkByScrollActionTest extends ESTestCase {
    private MockClearScrollClient client;
    private ThreadPool threadPool;
    private DummyAbstractBulkByScrollRequest mainRequest;
    private SearchRequest firstSearchRequest;
    private PlainActionFuture<Object> listener;
    private String scrollId;

    @Before
    public void setupForTest() {
        client = new MockClearScrollClient(new NoOpClient(getTestName()));
        threadPool = new ThreadPool(getTestName());
        mainRequest = new DummyAbstractBulkByScrollRequest();
        firstSearchRequest = null;
        listener = new PlainActionFuture<>();
        scrollId = null;
    }

    @After
    public void tearDownAndVerifyCommonStuff() {
        client.close();
        threadPool.shutdown();
        if (scrollId != null) {
            assertThat(client.scrollsCleared, contains(scrollId));
        }
    }

    /**
     * Generates a random scrollId and registers it so that when the test
     * finishes we check that it was cleared. Subsequent calls reregister a new
     * random scroll id so it is checked instead.
     */
    private String scrollId() {
        scrollId = randomSimpleString(random());
        return scrollId;
    }

    /**
     * Mimicks a ThreadPool rejecting execution of the task.
     */
    public void testThreadPoolRejectionsAbortRequest() throws Exception {
        threadPool.shutdown();
        threadPool = new ThreadPool(getTestName()) {
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
        new DummyAbstractAsyncBulkByScrollAction()
                .onScrollResponse(new SearchResponse(null, scrollId(), 5, 4, randomLong(), null));
        try {
            listener.get();
            fail("Expected a failure");
        } catch (ExecutionException e) {
            assertThat(e.getMessage(), equalTo("EsRejectedExecutionException[test]"));
        }
    }

    /**
     * Mimicks shard search failures usually caused by the data node serving the
     * scroll request going down.
     */
    public void testShardFailuresAbortRequest() throws Exception {
        ShardSearchFailure shardFailure = new ShardSearchFailure(new RuntimeException("test"));
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
        action.onScrollResponse(new SearchResponse(null, scrollId(), 5, 4, randomLong(), new ShardSearchFailure[] { shardFailure }));
        listener.get();
        assertThat(action.indexingFailures(), emptyCollectionOf(Failure.class));
        assertThat(action.searchFailures(), contains(shardFailure));
    }

    /**
     * Mimicks bulk indexing failures.
     */
    public void testBulkFailuresAbortRequest() throws Exception {
        Failure failure = new Failure("index", "type", "id", new RuntimeException("test"));
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
        action.onBulkResponse(new BulkResponse(new BulkItemResponse[] {new BulkItemResponse(0, "index", failure)}, randomLong()));
        listener.get();
        assertThat(action.indexingFailures(), contains(failure));
        assertThat(action.searchFailures(), emptyCollectionOf(ShardSearchFailure.class));
    }

    private class DummyAbstractAsyncBulkByScrollAction extends AbstractAsyncBulkByScrollAction<DummyAbstractBulkByScrollRequest, Object> {
        public DummyAbstractAsyncBulkByScrollAction() {
            super(logger, client, threadPool, AsyncBulkByScrollActionTest.this.mainRequest, firstSearchRequest, listener);
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

    private static class MockClearScrollClient extends FilterClient {
        private List<String> scrollsCleared = new ArrayList<>();

        public MockClearScrollClient(Client in) {
            super(in);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
                Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            if (request instanceof ClearScrollRequest) {
                ClearScrollRequest clearScroll = (ClearScrollRequest) request;
                scrollsCleared.addAll(clearScroll.getScrollIds());
                listener.onResponse((Response) new ClearScrollResponse(true, clearScroll.getScrollIds().size()));
                return;
            }
            super.doExecute(action, request, listener);
        }
    }
}
