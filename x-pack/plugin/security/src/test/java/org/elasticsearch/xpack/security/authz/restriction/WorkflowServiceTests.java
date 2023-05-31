/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.restriction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.restriction.Workflow;
import org.elasticsearch.xpack.core.security.authz.restriction.WorkflowResolver;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class WorkflowServiceTests extends ESTestCase {

    private final WorkflowService workflowService = new WorkflowService(MockLicenseState.createMock());

    public void testResolveWorkflowAndStoreInThreadContextWithKnownRestHandler() {
        final Workflow expectedWorkflow = randomFrom(WorkflowResolver.allWorkflows());
        final RestHandler restHandler;
        if (randomBoolean()) {
            restHandler = createBaseHandler(randomFrom(expectedWorkflow.allowedRestHandlers()));
        } else {
            restHandler = Mockito.mock(RestHandler.class);
            Mockito.when(restHandler.getConcreteRestHandler())
                .thenReturn(createBaseHandler(randomFrom(expectedWorkflow.allowedRestHandlers())));
        }
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        final AtomicBoolean calledListener = new AtomicBoolean(false);
        final AtomicReference<Workflow> workflowReference = new AtomicReference<>();
        final ActionListener<Workflow> listener = new ActionListener<>() {
            @Override
            public void onResponse(Workflow actual) {
                if (calledListener.compareAndSet(false, true) == false) {
                    fail("listener called more than once!");
                }
                workflowReference.set(actual);
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        };

        workflowService.resolveWorkflowAndStoreInThreadContext(restHandler, threadContext, listener);

        assertThat(calledListener.get(), equalTo(true));
        assertThat(workflowReference.get(), equalTo(expectedWorkflow));
        assertThat(threadContext.getHeader(WorkflowService.WORKFLOW_THREAD_CONTEXT_KEY), equalTo(expectedWorkflow.name()));
    }

    public void testResolveWorkflowAndStoreInThreadContextWithUnknownRestHandler() {
        final RestHandler restHandler;
        if (randomBoolean()) {
            String restHandlerName = randomValueOtherThanMany(
                name -> WorkflowResolver.resolveWorkflowForRestHandler(name) != null,
                () -> randomAlphaOfLengthBetween(3, 6)
            );
            restHandler = createBaseHandler(restHandlerName);
        } else {
            restHandler = Mockito.mock(RestHandler.class);
        }

        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final AtomicBoolean calledListener = new AtomicBoolean(false);
        final AtomicReference<Workflow> workflowReference = new AtomicReference<>();
        final ActionListener<Workflow> listener = new ActionListener<>() {
            @Override
            public void onResponse(Workflow actual) {
                if (calledListener.compareAndSet(false, true) == false) {
                    fail("listener called more than once!");
                }
                workflowReference.set(actual);
            }

            @Override
            public void onFailure(Exception e) {
                fail("unexpected failure: " + e.getMessage());
            }
        };

        workflowService.resolveWorkflowAndStoreInThreadContext(restHandler, threadContext, listener);

        assertThat(calledListener.get(), equalTo(true));
        assertThat(workflowReference.get(), nullValue());
        assertThat(threadContext.getHeader(WorkflowService.WORKFLOW_THREAD_CONTEXT_KEY), nullValue());
    }

    private BaseRestHandler createBaseHandler(String name) {
        return new BaseRestHandler() {

            @Override
            public String getName() {
                return name;
            }

            @Override
            public List<Route> routes() {
                return Collections.emptyList();
            }

            @Override
            protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                throw new UnsupportedOperationException("not implemented!");
            }

        };
    }

}
