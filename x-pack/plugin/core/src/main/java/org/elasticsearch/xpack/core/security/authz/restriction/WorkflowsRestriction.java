/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.restriction;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public record WorkflowsRestriction(Set<Workflow> workflows) {

    /**
     * Default behaviour is to allow access to all workflows if none are provided.
     */
    public static final WorkflowsRestriction ALLOW_ALL = new WorkflowsRestriction(Set.of());

    public WorkflowsRestriction(Set<Workflow> workflows) {
        this.workflows = Objects.requireNonNull(workflows);
    }

    public boolean hasWorkflows() {
        return this.workflows.size() > 0;
    }

    public boolean isWorkflowAllowed(Workflow workflow) {
        if (workflows.isEmpty()) {
            return true;
        }
        if (workflow == null) {
            return false;
        }
        return workflows.contains(workflow);
    }

    public static WorkflowsRestriction resolve(Set<String> names) {
        if (names == null || names.isEmpty()) {
            return WorkflowsRestriction.ALLOW_ALL;
        }
        final Set<Workflow> workflows = new HashSet<>(names.size());
        for (String name : names) {
            workflows.add(WorkflowResolver.resolveWorkflowByName(name));
        }
        return new WorkflowsRestriction(workflows);
    }

}
