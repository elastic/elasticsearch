/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.util.FeatureFlag;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public record WorkflowPermission(Set<Workflow> workflows) {

    public static final TransportVersion WORKFLOW_VERSION = TransportVersion.V_8_8_0;

    public static final FeatureFlag WORKFLOW_FEATURE_FLAG = new FeatureFlag("workflow_authorization");

    /**
     * Default behaviour is to allow access to all workflows if none are provided.
     */
    public static final WorkflowPermission ALLOW_ALL = new WorkflowPermission(Set.of());

    public WorkflowPermission(Set<Workflow> workflows) {
        this.workflows = Objects.requireNonNull(workflows);
    }

    public boolean checkEndpoint(String endpoint) {
        if (workflows.isEmpty()) {
            return true;
        }
        return workflows.stream().anyMatch(w -> w.checkEndpoint(endpoint));
    }

    /**
     * A workflow consists of a set of endpoints which are allowed to be accessed by an end-user.
     *
     * @param name a unique workflow name
     * @param allowedEndpoints set of allowed REST endpoints
     */
    public record Workflow(String name, Set<String> allowedEndpoints) {

        public Workflow(String name, Set<String> allowedEndpoints) {
            this.name = Objects.requireNonNull(name, "workflow name must be provided");
            this.allowedEndpoints = Set.copyOf(allowedEndpoints);
        }

        public boolean checkEndpoint(String endpoint) {
            return allowedEndpoints.contains(endpoint);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {

            private String name;
            private Set<String> allowedEndpoints = new HashSet<>();

            public Builder name(String name) {
                this.name = name;
                return this;
            }

            public Builder addEndpoint(String endpoint) {
                this.allowedEndpoints.add(endpoint);
                return this;
            }

            public Builder endpoints(String... endpoints) {
                for (String endpoint : endpoints) {
                    addEndpoint(endpoint);
                }
                return this;
            }

            public Workflow build() {
                return new Workflow(name, allowedEndpoints);
            }
        }
    }
}
