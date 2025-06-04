/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

public class TransportActionFilterChainRefCountingTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(TestPlugin.class);
    }

    static final ActionType<Response> TYPE = new ActionType<>("test:action");

    public void testAsyncActionFilterRefCounting() {
        final var countDownLatch = new CountDownLatch(2);
        final var request = new Request();
        try {
            client().execute(TYPE, request, ActionListener.<Response>running(countDownLatch::countDown).delegateResponse((delegate, e) -> {
                // _If_ we got an exception then it must be an ElasticsearchException with message "short-circuit failure", i.e. we're
                // checking that nothing else can go wrong here. But it's also ok for everything to succeed too, in which case we countDown
                // the latch without running this block.
                assertEquals("short-circuit failure", asInstanceOf(ElasticsearchException.class, e).getMessage());
                delegate.onResponse(null);
            }));
        } finally {
            request.decRef();
        }
        request.addCloseListener(ActionListener.running(countDownLatch::countDown));
        safeAwait(countDownLatch);
    }

    public static class TestPlugin extends Plugin implements ActionPlugin {

        private ThreadPool threadPool;

        @Override
        public Collection<?> createComponents(PluginServices services) {
            threadPool = services.threadPool();
            return List.of();
        }

        @Override
        public List<ActionHandler> getActions() {
            return List.of(new ActionHandler(TYPE, TestAction.class));
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return randomSubsetOf(
                List.of(
                    new TestAsyncActionFilter(threadPool),
                    new TestAsyncActionFilter(threadPool),
                    new TestAsyncMappedActionFilter(threadPool),
                    new TestAsyncMappedActionFilter(threadPool)
                )
            );
        }
    }

    private static class TestAsyncActionFilter implements ActionFilter {

        private final ThreadPool threadPool;
        private final int order = randomInt();

        private TestAsyncActionFilter(ThreadPool threadPool) {
            this.threadPool = Objects.requireNonNull(threadPool);
        }

        @Override
        public int order() {
            return order;
        }

        @Override
        public <Req extends ActionRequest, Rsp extends ActionResponse> void apply(
            Task task,
            String action,
            Req request,
            ActionListener<Rsp> listener,
            ActionFilterChain<Req, Rsp> chain
        ) {
            if (action.equals(TYPE.name())) {
                randomFrom(EsExecutors.DIRECT_EXECUTOR_SERVICE, threadPool.generic()).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        fail(e);
                    }

                    @Override
                    protected void doRun() {
                        assertTrue(request.hasReferences());
                        if (randomBoolean()) {
                            chain.proceed(task, action, request, listener);
                        } else {
                            listener.onFailure(new ElasticsearchException("short-circuit failure"));
                        }
                    }
                });
            } else {
                chain.proceed(task, action, request, listener);
            }
        }
    }

    private static class TestAsyncMappedActionFilter extends TestAsyncActionFilter implements MappedActionFilter {

        private TestAsyncMappedActionFilter(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        public String actionName() {
            return TYPE.name();
        }
    }

    public static class TestAction extends TransportAction<Request, Response> {

        private final ThreadPool threadPool;

        @Inject
        public TestAction(TransportService transportService, ActionFilters actionFilters) {
            super(TYPE.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
            threadPool = transportService.getThreadPool();
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            request.mustIncRef();
            threadPool.generic().execute(ActionRunnable.supply(ActionListener.runBefore(listener, request::decRef), () -> {
                assert request.hasReferences();
                return new Response();
            }));
        }
    }

    private static class Request extends LegacyActionRequest {
        private final SubscribableListener<Void> closeListeners = new SubscribableListener<>();
        private final RefCounted refs = LeakTracker.wrap(AbstractRefCounted.of(() -> closeListeners.onResponse(null)));

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void incRef() {
            refs.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refs.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refs.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refs.hasReferences();
        }

        void addCloseListener(ActionListener<Void> listener) {
            closeListeners.addListener(listener);
        }
    }

    private static class Response extends ActionResponse {
        @Override
        public void writeTo(StreamOutput out) {}
    }
}
