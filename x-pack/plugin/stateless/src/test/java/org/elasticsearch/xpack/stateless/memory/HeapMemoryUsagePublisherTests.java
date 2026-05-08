/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

public class HeapMemoryUsagePublisherTests extends ESTestCase {

    private static final String TRANSIENT_KEY = "_transient";
    private TestThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getClass().getSimpleName());
    }

    @After
    public void shutdownThreadPool() {
        Releasables.close(threadPool);
    }

    public void testRequestIsAlwaysSentInSystemContext() {
        HeapMemoryUsagePublisher heapMemoryUsagePublisher = new HeapMemoryUsagePublisher(new SystemContextAssertingClient(threadPool));
        ThreadContext threadContext = threadPool.getThreadContext();
        assertFalse(threadContext.isSystemContext());
        String transientValue = randomIdentifier();
        // This should not appear in the system context
        threadContext.putTransient(TRANSIENT_KEY, transientValue);
        safeAwait(l -> {
            heapMemoryUsagePublisher.publishIndicesMappingSize(new HeapMemoryUsage(1, Map.of()), l.map(ignored -> null));
            assertFalse(threadContext.isSystemContext());
            assertEquals(threadContext.getTransient(TRANSIENT_KEY), transientValue);
        });
    }

    private class SystemContextAssertingClient extends NoOpNodeClient {

        SystemContextAssertingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assert request instanceof PublishHeapMemoryMetricsRequest;
            ThreadContext threadContext = threadPool.getThreadContext();
            assertTrue(threadContext.isSystemContext());
            assertNull(threadContext.getTransient(TRANSIENT_KEY));
            super.doExecute(action, request, listener);
        }
    }
}
