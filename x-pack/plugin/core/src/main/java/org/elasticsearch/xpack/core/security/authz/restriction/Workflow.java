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

/**
* A workflow consists of a set of REST handlers which are allowed to be accessed by an end-user.
*
* @param name a unique workflow name
* @param allowedRestHandlers set of allowed REST handler names
*/
public record Workflow(String name, Set<String> allowedRestHandlers) {

    public Workflow(String name, Set<String> allowedRestHandlers) {
        assert false == allowedRestHandlers.isEmpty() : "allowed rest handlers must not be empty";
        this.name = Objects.requireNonNull(name, "workflow name must be provided");
        this.allowedRestHandlers = Set.copyOf(allowedRestHandlers);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String name;
        private final Set<String> allowedRestHandlers = new HashSet<>();

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder addAllowedRestHandler(String handler) {
            this.allowedRestHandlers.add(handler);
            return this;
        }

        public Builder addAllowedRestHandlers(String... handlers) {
            for (String handler : handlers) {
                addAllowedRestHandler(handler);
            }
            return this;
        }

        public Workflow build() {
            return new Workflow(name, allowedRestHandlers);
        }
    }
}
