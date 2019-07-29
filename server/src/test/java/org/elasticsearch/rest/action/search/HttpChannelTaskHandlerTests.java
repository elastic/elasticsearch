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
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

    /**
     * This test verifies that no tasks are left in the map where channels and their corresponding tasks are tracked.
     * Through the {@link TestClient} we simulate a scenario where the task listener can be called before the task has been
     * associated with its channel. Either way, we need to make sure that no tasks are left in the map.
     */
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
            assertEquals(numChannels, httpChannelTaskHandler.httpChannels.size());
            for (Map.Entry<HttpChannel, HttpChannelTaskHandler.CloseListener> entry : httpChannelTaskHandler.httpChannels.entrySet()) {
                assertEquals(0, entry.getValue().taskIds.size());
            }
        }
    }

    public void testChannelClose() throws Exception {
        try (TestClient testClient = new TestClient(Settings.EMPTY, threadPool)) {
            testClient.timeout.set(true);
            HttpChannelTaskHandler httpChannelTaskHandler = new HttpChannelTaskHandler();
            int numChannels = randomIntBetween(1, 30);
            int totalTasks = 0;
            List<TestHttpChannel> channels = new ArrayList<>(numChannels);
            for (int i = 0; i < numChannels; i++) {
                TestHttpChannel channel = new TestHttpChannel();
                channels.add(channel);
                int numTasks = randomIntBetween(1, 30);
                totalTasks += numTasks;
                for (int j = 0; j < numTasks; j++) {
                    httpChannelTaskHandler.execute(testClient, channel, new SearchRequest(), SearchAction.INSTANCE, null);
                }
                assertEquals(numTasks, httpChannelTaskHandler.httpChannels.get(channel).taskIds.size());
            }
            assertEquals(numChannels, httpChannelTaskHandler.httpChannels.size());
            for (TestHttpChannel channel : channels) {
                channel.awaitClose();
            }
            assertEquals(0, httpChannelTaskHandler.httpChannels.size());
            assertEquals(totalTasks, testClient.cancelledTasks.size());
        }
    }

    public void testChannelAlreadyClosed() {
        try (TestClient testClient = new TestClient(Settings.EMPTY, threadPool)) {
            testClient.timeout.set(true);

            int numChannels = randomIntBetween(1, 30);
            HttpChannelTaskHandler httpChannelTaskHandler = new HttpChannelTaskHandler();
            for (int i = 0; i < numChannels; i++) {
                TestHttpChannel channel = new TestHttpChannel();
                //no need to wait here, there will be no close listener registered, nothing to wait for.
                channel.close();
                //here the channel will be first registered, then straight-away removed from the map as the close listener is invoked
                //TODO is it possible that more tasks are started from a closed channel? In that case we would end up registering the close
                //listener multiple times as the channel is unknown
                httpChannelTaskHandler.execute(testClient, channel, new SearchRequest(), SearchAction.INSTANCE, null);
            }
            assertEquals(0, httpChannelTaskHandler.httpChannels.size());
            assertEquals(0, testClient.cancelledTasks.size());
        }
    }

    private static class TestClient extends NodeClient {
        private final AtomicLong counter = new AtomicLong(0);
        private final AtomicBoolean timeout = new AtomicBoolean(false);
        private final Set<TaskId> cancelledTasks = new CopyOnWriteArraySet<>();

        TestClient(Settings settings, ThreadPool threadPool) {
            super(settings, threadPool);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(ActionType<Response> action,
                                                                                                    Request request,
                                                                                                    TaskListener<Response> listener) {
            assert action == SearchAction.INSTANCE;
            Task task = request.createTask(counter.getAndIncrement(), "search", action.name(), null, Collections.emptyMap());
            if (timeout.get() == false) {
                if (rarely()) {
                    //make sure that search is sometimes also called from the same thread before the task is returned
                    listener.onResponse(task, null);
                } else {
                    threadPool().generic().submit(() -> listener.onResponse(task, null));
                }
            }
            return task;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(ActionType<Response> action,
                                                                                                    Request request,
                                                                                                    ActionListener<Response> listener) {
            assert action == CancelTasksAction.INSTANCE;
            CancelTasksRequest cancelTasksRequest = (CancelTasksRequest) request;
            assertTrue("tried to cancel the same task more than once", cancelledTasks.add(cancelTasksRequest.getTaskId()));
            Task task = request.createTask(counter.getAndIncrement(), "cancel_task", action.name(), null, Collections.emptyMap());
            if (randomBoolean()) {
                listener.onResponse(null);
            } else {
                //test that cancel tasks is best effort, failure received are not propagated
                listener.onFailure(new IllegalStateException());
            }

            return task;
        }

        @Override
        public String getLocalNodeId() {
            return "node";
        }
    }

    private class TestHttpChannel implements HttpChannel {
        private final AtomicBoolean open = new AtomicBoolean(true);
        private final AtomicReference<ActionListener<Void>> closeListener = new AtomicReference<>();
        private final CountDownLatch closeLatch = new CountDownLatch(1);

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
                boolean failure = randomBoolean();
                threadPool.generic().submit(() -> {
                    if (failure) {
                        listener.onFailure(new IllegalStateException());
                    } else {
                        listener.onResponse(null);
                    }
                    closeLatch.countDown();
                });
            }
        }

        private void awaitClose() throws InterruptedException {
            close();
            closeLatch.await();
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
            if (open.get() == false) {
                listener.onResponse(null);
            }
        }
    }
}
