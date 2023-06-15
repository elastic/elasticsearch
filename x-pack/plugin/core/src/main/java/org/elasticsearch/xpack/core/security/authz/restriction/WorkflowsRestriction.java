/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.restriction;

import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

public final class WorkflowsRestriction {

    /**
     * Default behaviour is to allow access to all workflows if none are provided.
     */
    public static final WorkflowsRestriction NONE = new WorkflowsRestriction(Set.of());

    private final Set<String> names;
    private final Predicate<String> predicate;

    public WorkflowsRestriction(Set<String> names) {
        this.names = Objects.requireNonNull(names);
        this.predicate = StringMatcher.of(names);
    }

    public boolean hasWorkflows() {
        return this.names.size() > 0;
    }

    public boolean isWorkflowAllowed(String workflow) {
        if (names.isEmpty()) {
            return true;
        }
        if (workflow == null) {
            return false;
        }
        return predicate.test(workflow);
    }

    public static WorkflowsRestriction resolve(Set<String> names) {
        if (names == null || names.isEmpty()) {
            return WorkflowsRestriction.NONE;
        }
        return new WorkflowsRestriction(names);
    }

}
