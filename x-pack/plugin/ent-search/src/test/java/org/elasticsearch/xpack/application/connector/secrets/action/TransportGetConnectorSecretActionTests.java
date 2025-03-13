/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.secrets.ConnectorSecretsTestUtils;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

public class TransportGetConnectorSecretActionTests extends ESSingleNodeTestCase {

    private static final Long TIMEOUT_SECONDS = 10L;

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());
    private TransportGetConnectorSecretAction action;

    @Before
    public void setup() {
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );

        action = new TransportGetConnectorSecretAction(transportService, mock(ActionFilters.class), client());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    public void testGetConnectorSecret_ExpectNoWarnings() throws InterruptedException {
        GetConnectorSecretRequest request = ConnectorSecretsTestUtils.getRandomGetConnectorSecretRequest();

        executeRequest(request);

        ensureNoWarnings();
    }

    private void executeRequest(GetConnectorSecretRequest request) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        action.doExecute(mock(Task.class), request, ActionListener.wrap(response -> latch.countDown(), exception -> latch.countDown()));

        boolean requestTimedOut = latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue("Timeout waiting for get request", requestTimedOut);
    }
}
