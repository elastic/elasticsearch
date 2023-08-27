/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.downsampling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;

public class ReplaceBackingWithDownsampleIndexExecutorTests extends ESTestCase {

    public void testExecutorDeletesTheSourceIndexWhenTaskSucceeds() {
        String dataStreamName = randomAlphaOfLengthBetween(10, 100);
        String sourceIndex = randomAlphaOfLengthBetween(10, 100);
        String downsampleIndex = randomAlphaOfLengthBetween(10, 100);

        try (Client client = new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertThat(action.name(), is(DeleteIndexAction.NAME));
                assertTrue(request instanceof DeleteIndexRequest);
                DeleteIndexRequest deleteIndexRequest = (DeleteIndexRequest) request;
                assertThat(deleteIndexRequest.indices().length, is(1));
                assertThat(deleteIndexRequest.indices()[0], is(sourceIndex));
            }
        }) {
            ReplaceBackingWithDownsampleIndexExecutor executor = new ReplaceBackingWithDownsampleIndexExecutor(client);

            AtomicBoolean taskListenerCalled = new AtomicBoolean(false);
            executor.taskSucceeded(
                new ReplaceSourceWithDownsampleIndexTask(dataStreamName, sourceIndex, downsampleIndex, new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void unused) {
                        taskListenerCalled.set(true);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(e.getMessage(), e);
                        fail("unexpected exception: " + e.getMessage());
                    }
                }),
                null
            );
            assertThat(taskListenerCalled.get(), is(true));
        }
    }

    public void testExecutorCallsTaskListenerEvenIfDeteleFails() {
        String dataStreamName = randomAlphaOfLengthBetween(10, 100);
        String sourceIndex = randomAlphaOfLengthBetween(10, 100);
        String downsampleIndex = randomAlphaOfLengthBetween(10, 100);

        try (Client client = new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onFailure(new IllegalStateException("simulating a failure to delete index " + sourceIndex));
            }
        }) {
            ReplaceBackingWithDownsampleIndexExecutor executor = new ReplaceBackingWithDownsampleIndexExecutor(client);

            AtomicBoolean taskListenerCalled = new AtomicBoolean(false);
            executor.taskSucceeded(
                new ReplaceSourceWithDownsampleIndexTask(dataStreamName, sourceIndex, downsampleIndex, new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void unused) {
                        taskListenerCalled.set(true);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(e.getMessage(), e);
                        fail("unexpected exception: " + e.getMessage());
                    }
                }),
                null
            );
            assertThat(taskListenerCalled.get(), is(true));
        }
    }
}
