/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.restriction;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class WorkflowResolverTests extends ESTestCase {

    public void testResolveWorkflowByName() {
        String invalidWorkflowName = randomValueOtherThanMany(
            name -> WorkflowResolver.names().contains(name),
            () -> randomAlphaOfLengthBetween(5, 10)
        );
        var e = expectThrows(IllegalArgumentException.class, () -> WorkflowResolver.resolveWorkflowByName(invalidWorkflowName));
        assertThat(e.getMessage(), containsString("Unknown workflow [" + invalidWorkflowName + "]"));

        String validWorkflowName = randomFrom(WorkflowResolver.names());
        Workflow resolvedWorkflow = WorkflowResolver.resolveWorkflowByName(validWorkflowName);
        assertThat(resolvedWorkflow.name(), equalTo(validWorkflowName));
    }

    public void testResolveWorkflowForRestHandler() {
        Workflow actual = WorkflowResolver.resolveWorkflowForRestHandler("search_application_query_action");
        assertThat(actual, equalTo(WorkflowResolver.SEARCH_APPLICATION_QUERY_WORKFLOW));

        actual = WorkflowResolver.resolveWorkflowForRestHandler("analytics_post_event_action");
        assertThat(actual, equalTo(WorkflowResolver.SEARCH_APPLICATION_ANALYTICS_WORKFLOW));

        assertThat(WorkflowResolver.resolveWorkflowForRestHandler(randomAlphaOfLengthBetween(3, 5)), nullValue());
    }
}
