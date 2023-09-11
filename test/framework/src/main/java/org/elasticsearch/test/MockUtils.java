/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

/**
 * Utilities for setting up Mockito mocks.
 */
public class MockUtils {

    /**
     * Sets up a mock TransportService that can answer calls to TransportService.getThreadPool().executor(String).
     *
     * @return A mocked TransportService instance
     */
    public static TransportService setupTransportServiceWithThreadpoolExecutor() {
        TransportService transportService = mock(TransportService.class);
        ThreadPool threadPool = mock(ThreadPool.class);

        setMockReturns(transportService, threadPool);

        return transportService;
    }

    /**
     * Sets up a mock TransportService that can answer calls to TransportService.getThreadPool().executor(String), using the given
     * threadPool in TransportService.
     *
     * @param threadPool A mock ThreadPool
     * @return A mocked TransportService instance
     */
    public static TransportService setupTransportServiceWithThreadpoolExecutor(ThreadPool threadPool) {
        assert mockingDetails(threadPool).isMock();
        TransportService transportService = mock(TransportService.class);

        setMockReturns(transportService, threadPool);

        return transportService;
    }

    /**
     * Sets up the given mock TransportService so that it can answer calls to TransportService.getThreadPool().executor(String).
     *
     * @param transportService A mock TransportService to be set up.
     */
    public static void setupTransportServiceWithThreadpoolExecutor(TransportService transportService) {
        assert mockingDetails(transportService).isMock();
        ThreadPool threadPool = mock(ThreadPool.class);

        setMockReturns(transportService, threadPool);
    }

    private static void setMockReturns(TransportService transportService, ThreadPool threadPool) {
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }
}
