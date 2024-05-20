/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobTestUtils;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

public class TransportListConnectorSyncJobsActionTests extends ESSingleNodeTestCase {
    private static final Long TIMEOUT_SECONDS = 10L;

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());
    private TransportListConnectorSyncJobsAction action;

    @Before
    public void setup() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );

        action = new TransportListConnectorSyncJobsAction(transportService, mock(ActionFilters.class), client());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    public void testListConnectorSyncJobs_ExpectNoWarnings() throws InterruptedException {
        ListConnectorSyncJobsAction.Request request = ConnectorSyncJobTestUtils.getRandomListConnectorSyncJobsActionRequest();

        executeRequest(request);

        ensureNoWarnings();
    }

    private void executeRequest(ListConnectorSyncJobsAction.Request request) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        action.doExecute(mock(Task.class), request, ActionListener.wrap(response -> latch.countDown(), exception -> latch.countDown()));

        boolean requestTimedOut = latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue("Timeout waiting for list request", requestTimedOut);
    }
}
