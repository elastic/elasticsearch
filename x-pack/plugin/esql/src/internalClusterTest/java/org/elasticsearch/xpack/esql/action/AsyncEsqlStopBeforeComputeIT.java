/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.asyncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * STOP issued before compute starts (field_caps still in flight) must be remembered so the query
 * finishes empty with {@code is_partial:true} instead of running to completion.
 */
public class AsyncEsqlStopBeforeComputeIT extends AbstractPausableIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return Collections.unmodifiableList(plugins);
    }

    public void testStopBeforeComputeDuringFieldCaps() throws Exception {
        CountDownLatch fieldCapsArrived = new CountDownLatch(1);
        CountDownLatch fieldCapsHold = new CountDownLatch(1);
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService ts = MockTransportService.getInstance(node);
            for (String action : List.of(TransportFieldCapabilitiesAction.NAME, TransportFieldCapabilitiesAction.ACTION_NODE_NAME)) {
                ts.addRequestHandlingBehavior(action, (handler, request, channel, task) -> {
                    fieldCapsArrived.countDown();
                    try {
                        if (fieldCapsHold.await(60, TimeUnit.SECONDS) == false) {
                            channel.sendResponse(new IllegalStateException("field_caps hold was not released"));
                            return;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        channel.sendResponse(e);
                        return;
                    }
                    handler.messageReceived(request, channel, task);
                });
            }
        }
        try {
            var request = asyncEsqlQueryRequest("FROM test | KEEP foo | LIMIT 10").waitForCompletionTimeout(TimeValue.timeValueNanos(1))
                .keepOnCompletion(true)
                .keepAlive(TimeValue.timeValueDays(1));
            try (var initialResponse = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(60, TimeUnit.SECONDS)) {
                assertThat(initialResponse.isRunning(), is(true));
                assertThat(initialResponse.asyncExecutionId(), isPresent());
                assertTrue("field_caps must start before STOP", fieldCapsArrived.await(30, TimeUnit.SECONDS));

                var stopFuture = client().execute(
                    EsqlAsyncStopAction.INSTANCE,
                    new AsyncStopRequest(initialResponse.asyncExecutionId().get())
                );
                assertBusy(() -> assertTrue("STOP must set isStopped before field_caps is released", queryMarkedStopped()));
                fieldCapsHold.countDown();
                try (var stoppedResponse = stopFuture.actionGet(30, TimeUnit.SECONDS)) {
                    assertThat(stoppedResponse.isRunning(), is(false));
                    assertThat(stoppedResponse.isPartial(), is(true));
                    assertThat(getValuesList(stoppedResponse), empty());
                }
            }
        } finally {
            fieldCapsHold.countDown();
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
            scriptPermits.drainPermits();
            scriptWaits.drainPermits();
        }
    }

    private boolean queryMarkedStopped() {
        for (String node : internalCluster().getNodeNames()) {
            for (Task task : internalCluster().getInstance(TransportService.class, node).getTaskManager().getCancellableTasks().values()) {
                if (task instanceof EsqlQueryTask esql && esql.executionInfo() != null && esql.executionInfo().isStopped()) {
                    return true;
                }
            }
        }
        return false;
    }
}
