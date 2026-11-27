/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.EsqlIntoDestinationAction;
import org.elasticsearch.xpack.esql.action.EsqlIntoReplaceDestinationAction;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class IntoDestinationAuthorizerTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testTheFilterIsAskedAboutTheDestination() {
        RecordingClient client = new RecordingClient(threadPool, null);
        authorize(client, "dest", DocWriteRequest.OpType.CREATE).actionGet();

        assertThat(client.actions, equalTo(List.of(EsqlIntoDestinationAction.NAME)));
        assertThat(((EsqlIntoDestinationAction.Request) client.requests.getFirst()).indices(), equalTo(new String[] { "dest" }));
    }

    /** A write that replaces sends index requests, which the filter authorizes as another action than a create. */
    public void testTheFilterIsAskedToAuthorizeAnIndexRequestWhenTheWriteReplaces() {
        RecordingClient client = new RecordingClient(threadPool, null);
        authorize(client, "dest", DocWriteRequest.OpType.INDEX).actionGet();

        assertThat(client.actions, equalTo(List.of(EsqlIntoReplaceDestinationAction.NAME)));
        assertThat(((EsqlIntoDestinationAction.Request) client.requests.getFirst()).indices(), equalTo(new String[] { "dest" }));
    }

    public void testTheDenialOfTheFilterIsTheDenialOfTheQuery() throws Exception {
        ElasticsearchSecurityException denial = new ElasticsearchSecurityException("denied", RestStatus.FORBIDDEN);
        PlainActionFuture<Void> future = authorize(new RecordingClient(threadPool, denial), "dest", DocWriteRequest.OpType.CREATE);

        ExecutionException e = expectThrows(ExecutionException.class, future::get);
        assertThat(e.getCause(), sameInstance(denial));
    }

    /** The filter reads a leading slash as a regular expression and a star as a wildcard, which would authorize more than one index. */
    public void testNameTheFilterWouldReadAsAPatternIsRejectedWithoutAskingIt() {
        for (String name : List.of("/.*/", "/dest/", "dest*", "*", "-dest", "a,b", "remote:dest", "")) {
            RecordingClient client = new RecordingClient(threadPool, null);
            PlainActionFuture<Void> future = authorize(client, name, DocWriteRequest.OpType.CREATE);

            ExecutionException e = expectThrows(ExecutionException.class, future::get);
            assertThat(name, e.getCause(), instanceOf(InvalidIndexNameException.class));
            assertThat(name, ((InvalidIndexNameException) e.getCause()).status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(name, client.requests, empty());
        }
    }

    private static PlainActionFuture<Void> authorize(RecordingClient client, String destination, DocWriteRequest.OpType opType) {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        IntoDestinationAuthorizer.create(client, EsExecutors.DIRECT_EXECUTOR_SERVICE).authorize(destination, opType, future);
        return future;
    }

    /** Records what is sent through it, and either answers it or fails it. A client is needed since the action runs on a node. */
    private static final class RecordingClient extends NoOpClient {
        final List<String> actions = new ArrayList<>();
        final List<ActionRequest> requests = new ArrayList<>();
        @Nullable
        private final Exception failure;

        RecordingClient(ThreadPool threadPool, @Nullable Exception failure) {
            super(threadPool);
            this.failure = failure;
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            actions.add(action.name());
            requests.add(request);
            if (failure != null) {
                listener.onFailure(failure);
            } else {
                super.doExecute(action, request, listener);
            }
        }
    }
}
