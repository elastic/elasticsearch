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

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class HttpChannelTaskHandlerTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(HttpChannelTaskHandlerTests.class.getName());
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
    }

    public void testLinkAndUnlink() throws Exception {

        try (TestClient testClient = new TestClient(Settings.EMPTY, threadPool)) {
            HttpChannelTaskHandler httpChannelTaskHandler = new HttpChannelTaskHandler();
            List<Future<?>> futures = new ArrayList<>();
            int numChannels = randomIntBetween(1, 30);
            for (int i = 0; i < numChannels; i++) {
                int numTasks = randomIntBetween(1, 30);
                TestHttpChannel channel = new TestHttpChannel();
                for (int j = 0; j < numTasks; j++) {
                    PlainListenableActionFuture<SearchResponse> actionFuture = PlainListenableActionFuture.newListenableFuture();
                    threadPool.generic().submit(() -> httpChannelTaskHandler.execute(testClient, channel, new SearchRequest(),
                        SearchAction.INSTANCE, actionFuture));
                    futures.add(actionFuture);
                }
            }

            for (Future<?> future : futures) {
                future.get();
            }
            //no channels get closed in this test
            assertThat(httpChannelTaskHandler.httpChannels.size(), lessThanOrEqualTo(numChannels));
            for (Map.Entry<HttpChannel, HttpChannelTaskHandler.CloseListener> entry : httpChannelTaskHandler.httpChannels.entrySet()) {
                assertEquals(0, entry.getValue().taskIds.size());
            }
        }
    }

    //TODO verify that we do not add stuff to the map when the channel is closed at link call
    //same when unlink is called before link

    private static class TestClient extends NodeClient {

        private final AtomicLong counter = new AtomicLong(0);

        TestClient(Settings settings, ThreadPool threadPool) {
            super(settings, threadPool);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(ActionType<Response> action,
                                                                                                    Request request,
                                                                                                    TaskListener<Response> listener) {
            Task task = request.createTask(counter.getAndIncrement(), "type", action.name(), null, Collections.emptyMap());
            if (rarely()) {
                listener.onResponse(task, null);
            } else {
                threadPool().generic().submit(() -> listener.onResponse(task, null));
            }
            return task;
        }

        @Override
        public String getLocalNodeId() {
            return "node";
        }
    }

    private static class TestHttpChannel implements HttpChannel {
        private final AtomicBoolean open = new AtomicBoolean(true);
        private final AtomicReference<ActionListener<Void>> closeListener = new AtomicReference<>();

        @Override
        public void sendResponse(HttpResponse response, ActionListener<Void> listener) {

        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public void close() {
            if (open.compareAndSet(true, false) == false) {
                throw new IllegalStateException("channel already closed!");
            }
            ActionListener<Void> listener = closeListener.get();
            if (listener != null) {
                listener.onResponse(null);
            }
        }

        @Override
        public boolean isOpen() {
            return open.get();
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            if (closeListener.compareAndSet(null, listener) == false) {
                throw new IllegalStateException("close listener already set, only one is allowed!");
            }
        }
    }
}
