/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.restriction;

import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.Set;
import java.util.function.Predicate;

public final class WorkflowsRestriction {

    /**
     * Default behaviour is to allow access to all workflows if none are provided.
     */
    public static final WorkflowsRestriction NONE = new WorkflowsRestriction(null);

    private final Set<String> names;
    private final Predicate<String> predicate;

    public WorkflowsRestriction(Set<String> names) {
        assert names == null || names.size() > 0 : "workflow names cannot be an empty set";
        this.names = names;
        if (names != null) {
            this.predicate = StringMatcher.of(names);
        } else {
            this.predicate = name -> true;
        }
    }

    public boolean hasWorkflows() {
        return this.names != null && this.names.size() > 0;
    }

    public boolean isWorkflowAllowed(String workflow) {
        return predicate.test(workflow);
    }

    public static WorkflowsRestriction resolve(Set<String> names) {
        if (names == null || names.isEmpty()) {
            return WorkflowsRestriction.NONE;
        }
        return new WorkflowsRestriction(names);
    }

}
