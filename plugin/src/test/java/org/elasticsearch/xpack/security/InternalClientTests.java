/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class InternalClientTests extends ESTestCase {
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(InternalClientTests.class.getName());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testContextIsPreserved() throws IOException, InterruptedException {
        FilterClient dummy = new FilterClient(Settings.EMPTY, threadPool, null) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends
                    ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder>
                                                                                                    action, Request request,
                                                                                            ActionListener<Response> listener) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> listener.onResponse(null));
            }
        };

        InternalClient client = new InternalClient(Settings.EMPTY, threadPool, dummy) {
            @Override
            protected void processContext(ThreadContext threadContext) {
                threadContext.putTransient("foo", "boom");
            }
        };
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putTransient("foo", "bar");
            client.prepareSearch("boom").get();
            assertEquals("bar", threadPool.getThreadContext().getTransient("foo"));
        }

        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putTransient("foo", "bar");
            CountDownLatch latch = new CountDownLatch(1);
            client.prepareSearch("boom").execute(new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    try {
                        assertEquals("bar", threadPool.getThreadContext().getTransient("foo"));
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        throw new AssertionError(e);
                    } finally {
                        latch.countDown();
                    }

                }
            });
            latch.await();
            assertEquals("bar", threadPool.getThreadContext().getTransient("foo"));
        }
    }

    public void testContextIsPreservedOnError() throws IOException, InterruptedException {
        FilterClient dummy = new FilterClient(Settings.EMPTY, threadPool, null) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends
                    ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder>
                                                                                                    action, Request request,
                                                                                            ActionListener<Response> listener) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> listener.onFailure(new Exception("boom bam bang")));
            }
        };

        InternalClient client = new InternalClient(Settings.EMPTY, threadPool, dummy) {
            @Override
            protected void processContext(ThreadContext threadContext) {
                threadContext.putTransient("foo", "boom");
            }
        };
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putTransient("foo", "bar");
            try {
                client.prepareSearch("boom").get();
            } catch (Exception ex) {
                assertEquals("boom bam bang", ex.getCause().getCause().getMessage());

            }
            assertEquals("bar", threadPool.getThreadContext().getTransient("foo"));
        }

        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putTransient("foo", "bar");
            CountDownLatch latch = new CountDownLatch(1);
            client.prepareSearch("boom").execute(new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    try {
                        throw new AssertionError("exception expected");
                    } finally {
                        latch.countDown();
                    }

                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        assertEquals("boom bam bang", e.getMessage());
                        assertEquals("bar", threadPool.getThreadContext().getTransient("foo"));
                    } finally {
                        latch.countDown();
                    }

                }
            });
            latch.await();
            assertEquals("bar", threadPool.getThreadContext().getTransient("foo"));
        }
    }
}
