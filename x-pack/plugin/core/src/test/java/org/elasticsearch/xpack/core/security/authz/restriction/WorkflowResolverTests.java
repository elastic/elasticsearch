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
            name -> WorkflowResolver.allWorkflows().stream().anyMatch(w -> w.name().equals(name)),
            () -> randomAlphaOfLengthBetween(5, 10)
        );
        var e = expectThrows(IllegalArgumentException.class, () -> WorkflowResolver.resolveWorkflowByName(invalidWorkflowName));
        assertThat(e.getMessage(), containsString("Unknown workflow [" + invalidWorkflowName + "]"));

        String validWorkflowName = randomFrom(WorkflowResolver.allWorkflows().stream().map(Workflow::name).toList());
        Workflow resolvedWorkflow = WorkflowResolver.resolveWorkflowByName(validWorkflowName);
        assertThat(resolvedWorkflow.name(), equalTo(validWorkflowName));
    }

    public void testResolveWorkflowForRestHandler() {
        Workflow actual = WorkflowResolver.resolveWorkflowForRestHandler("search_application_query_action");
        assertThat(actual, equalTo(WorkflowResolver.SEARCH_APPLICATION_QUERY_WORKFLOW));

        assertThat(WorkflowResolver.resolveWorkflowForRestHandler(randomAlphaOfLengthBetween(3, 5)), nullValue());
    }
}
