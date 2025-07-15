/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.xpack.core.security.SecurityContext;

import java.util.List;

public interface CrossProjectTargetResolver {
    ResolvedProjects resolve(SecurityContext securityContext);

    class Default implements CrossProjectTargetResolver {
        @Override
        public ResolvedProjects resolve(SecurityContext securityContext) {
            return ResolvedProjects.VOID;
        }
    }

    record ResolvedProjects(List<String> projects) {
        // I need a better name
        public static ResolvedProjects VOID = new ResolvedProjects(List.of());
    }
}
