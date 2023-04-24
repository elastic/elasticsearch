/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.permission.WorkflowPermission.Workflow;
import org.hamcrest.Matchers;

public class WorkflowPermissionResolverTests extends ESTestCase {

    public void testResolveWorkflow() {
        String invalidWorkflowName = randomValueOtherThanMany(
            name -> WorkflowPermissionResolver.names().contains(name),
            () -> randomAlphaOfLengthBetween(5, 10)
        );
        var e = expectThrows(IllegalArgumentException.class, () -> WorkflowPermissionResolver.resolveWorkflow(invalidWorkflowName));
        assertThat(
            e.getMessage(),
            Matchers.containsString(
                "Unknown workflow ["
                    + invalidWorkflowName
                    + "]. A workflow must be one of the predefined workflow names [search_application]."
            )
        );

        String validWorkflowName = randomFrom(WorkflowPermissionResolver.names().toArray(String[]::new));
        Workflow resolvedWorkflow = WorkflowPermissionResolver.resolveWorkflow(validWorkflowName);
        assertThat(resolvedWorkflow.name(), Matchers.equalTo(validWorkflowName));
    }
}
