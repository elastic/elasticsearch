/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.restriction;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.restriction.Workflow;
import org.elasticsearch.xpack.core.security.authz.restriction.WorkflowResolver;
import org.elasticsearch.xpack.security.authz.restriction.WorkflowService;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class WorkflowServiceTests extends ESTestCase {

    private final WorkflowService workflowService = new WorkflowService();

    public void testResolveWorkflowAndStoreInThreadContextWithKnownRestHandler() {
        final Workflow expectedWorkflow = randomFrom(WorkflowResolver.allWorkflows());
        final RestHandler restHandler = new TestBaseRestHandler(randomFrom(expectedWorkflow.allowedRestHandlers()));
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        final Workflow actualWorkflow = workflowService.resolveWorkflowAndStoreInThreadContext(restHandler, threadContext);
        assertThat(actualWorkflow, equalTo(expectedWorkflow));
        assertThat(workflowService.readWorkflowFromThreadContext(threadContext), equalTo(expectedWorkflow));
    }

    public void testResolveWorkflowAndStoreInThreadContextWithUnknownRestHandler() {
        final RestHandler restHandler;
        if (randomBoolean()) {
            String restHandlerName = randomValueOtherThanMany(
                name -> WorkflowResolver.resolveWorkflowForRestHandler(name) != null,
                () -> randomAlphaOfLengthBetween(3, 6)
            );
            restHandler = new TestBaseRestHandler(restHandlerName);
        } else {
            restHandler = Mockito.mock(RestHandler.class);
        }
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        final Workflow actualWorkflow = workflowService.resolveWorkflowAndStoreInThreadContext(restHandler, threadContext);
        assertThat(actualWorkflow, nullValue());
        assertThat(workflowService.readWorkflowFromThreadContext(threadContext), nullValue());
    }

    public static class TestBaseRestHandler extends BaseRestHandler {
        private final String name;

        public TestBaseRestHandler(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public List<Route> routes() {
            return List.of();
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            return channel -> {};
        }

    }

}
