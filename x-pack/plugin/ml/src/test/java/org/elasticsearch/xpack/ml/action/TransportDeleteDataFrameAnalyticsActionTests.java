/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.DeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.sameInstance;

public class TransportDeleteDataFrameAnalyticsActionTests extends ESTestCase {

    private static final String ID = "my-analytics";

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    /**
     * When the analytics does not exist, both the normal and the force stop fail with a
     * {@link ResourceNotFoundException}. The caller gets the 404, and nothing is logged at ERROR.
     */
    public void testStopJobWhenAnalyticsMissingDoesNotLogError() {
        ResourceNotFoundException notFound = new ResourceNotFoundException("No known data frame analytics with id [{}]", ID);
        StopFailingClient client = new StopFailingClient(threadPool, notFound);

        try (MockLog mockLog = MockLog.capture(TransportDeleteDataFrameAnalyticsAction.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no error", TransportDeleteDataFrameAnalyticsAction.class.getName(), Level.ERROR, "*")
            );

            PlainActionFuture<StopDataFrameAnalyticsAction.Response> future = new PlainActionFuture<>();
            TransportDeleteDataFrameAnalyticsAction.stopJob(
                new ParentTaskAssigningClient(client, new TaskId("node", 1)),
                new DeleteDataFrameAnalyticsAction.Request(ID),
                future
            );

            assertThat(expectThrows(ResourceNotFoundException.class, future::actionGet), sameInstance(notFound));
            mockLog.assertAllExpectationsMatched();
        }
        assertThat(client.forceFlags, contains(false, true));
    }

    /**
     * Stop failures other than a missing analytics are unexpected and are still logged at ERROR.
     */
    public void testStopJobOtherFailureLogsError() {
        ElasticsearchException failure = new ElasticsearchException("stop failed");
        StopFailingClient client = new StopFailingClient(threadPool, failure);

        try (MockLog mockLog = MockLog.capture(TransportDeleteDataFrameAnalyticsAction.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "normal stop error",
                    TransportDeleteDataFrameAnalyticsAction.class.getName(),
                    Level.ERROR,
                    "[" + ID + "] Failed to stop normally"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "force stop error",
                    TransportDeleteDataFrameAnalyticsAction.class.getName(),
                    Level.ERROR,
                    "[" + ID + "] Failed to stop forcefully"
                )
            );

            PlainActionFuture<StopDataFrameAnalyticsAction.Response> future = new PlainActionFuture<>();
            TransportDeleteDataFrameAnalyticsAction.stopJob(
                new ParentTaskAssigningClient(client, new TaskId("node", 1)),
                new DeleteDataFrameAnalyticsAction.Request(ID),
                future
            );

            assertThat(expectThrows(ElasticsearchException.class, future::actionGet), sameInstance(failure));
            mockLog.assertAllExpectationsMatched();
        }
        assertThat(client.forceFlags, contains(false, true));
    }

    /**
     * A client that fails every stop request with the given exception and records whether each one was a force stop.
     */
    private static class StopFailingClient extends NoOpClient {

        private final Exception failure;
        private final List<Boolean> forceFlags = new ArrayList<>();

        StopFailingClient(ThreadPool threadPool, Exception failure) {
            super(threadPool);
            this.failure = failure;
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, sameInstance(StopDataFrameAnalyticsAction.INSTANCE));
            forceFlags.add(((StopDataFrameAnalyticsAction.Request) request).isForce());
            listener.onFailure(failure);
        }
    }
}
