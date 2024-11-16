/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
        return setMockReturns(mock(TransportService.class), mock(ThreadPool.class));
    }

    /**
     * Sets up a mock TransportService that can answer calls to TransportService.getThreadPool().executor(String), using the given
     * threadPool in TransportService.
     *
     * @param threadPool A mock ThreadPool
     * @return A mocked TransportService instance
     */
    public static TransportService setupTransportServiceWithThreadpoolExecutor(ThreadPool threadPool) {
        return setMockReturns(mock(TransportService.class), threadPool);
    }

    private static TransportService setMockReturns(TransportService transportService, ThreadPool threadPool) {
        assert mockingDetails(threadPool).isMock();
        assert mockingDetails(transportService).isMock();
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        return transportService;
    }
}
