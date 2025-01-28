/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.restriction;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class WorkflowsRestrictionTests extends ESTestCase {

    public void testIsWorkflowAllowedAllowsAllWithNoRestrictions() {
        WorkflowsRestriction restriction = WorkflowsRestriction.NONE;
        String workflow = randomFrom(WorkflowResolver.SEARCH_APPLICATION_QUERY_WORKFLOW.name(), null, "", randomAlphaOfLength(10));
        assertThat(restriction.isWorkflowAllowed(workflow), equalTo(true));
    }

    public void testIsWorkflowAllowedAllowsOnlyRestrictedWorkflow() {
        String workflow = WorkflowResolver.SEARCH_APPLICATION_QUERY_WORKFLOW.name();
        WorkflowsRestriction restriction = new WorkflowsRestriction(Set.of(workflow));
        assertThat(restriction.isWorkflowAllowed(workflow), equalTo(true));
        assertThat(restriction.isWorkflowAllowed(randomFrom(randomAlphaOfLength(10), null, "")), equalTo(false));
    }
}
