/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.restriction;

import org.elasticsearch.core.Nullable;

import java.util.Set;
import java.util.function.Predicate;

public final class WorkflowsRestriction {

    /**
     * Default behaviour is no restriction which allows all workflows.
     */
    public static final WorkflowsRestriction NONE = new WorkflowsRestriction(null);

    private final Set<String> names;
    private final Predicate<String> predicate;

    public WorkflowsRestriction(Set<String> names) {
        this.names = names;
        if (names == null) {
            // No restriction, all workflows are allowed
            this.predicate = name -> true;
        } else if (names.isEmpty()) {
            // Empty restriction, no workflow is allowed
            this.predicate = name -> false;
        } else {
            this.predicate = name -> {
                if (name == null) {
                    return false;
                } else {
                    return names.contains(name);
                }
            };
        }
    }

    public boolean hasWorkflows() {
        return this.names != null;
    }

    public boolean isWorkflowAllowed(@Nullable String workflow) {
        return predicate.test(workflow);
    }

}
