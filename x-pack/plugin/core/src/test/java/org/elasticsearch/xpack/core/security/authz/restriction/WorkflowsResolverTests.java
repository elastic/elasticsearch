/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.restriction;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class WorkflowsResolverTests extends ESTestCase {

    public void testResolveWorkflowByName() {
        String invalidWorkflowName = randomValueOtherThanMany(
            name -> WorkflowsResolver.names().contains(name),
            () -> randomAlphaOfLengthBetween(5, 10)
        );
        var e = expectThrows(IllegalArgumentException.class, () -> WorkflowsResolver.resolveWorkflowByName(invalidWorkflowName));
        assertThat(
            e.getMessage(),
            Matchers.containsString(
                "Unknown workflow ["
                    + invalidWorkflowName
                    + "]. A workflow must be one of the predefined workflow names [search_application_query,search_application_analytics]."
            )
        );

        String validWorkflowName = randomFrom(WorkflowsResolver.names());
        Workflow resolvedWorkflow = WorkflowsResolver.resolveWorkflowByName(validWorkflowName);
        assertThat(resolvedWorkflow.name(), equalTo(validWorkflowName));
    }

    public void testResolveWorkflowForRestHandler() {
        Workflow actual = WorkflowsResolver.resolveWorkflowForRestHandler("search_application_query_action");
        assertThat(actual, equalTo(WorkflowsResolver.SEARCH_APPLICATION_QUERY_WORKFLOW));

        actual = WorkflowsResolver.resolveWorkflowForRestHandler("analytics_post_event_action");
        assertThat(actual, equalTo(WorkflowsResolver.SEARCH_APPLICATION_ANALYTICS_WORKFLOW));

        assertThat(WorkflowsResolver.resolveWorkflowForRestHandler(randomAlphaOfLengthBetween(3, 5)), nullValue());
    }
}
