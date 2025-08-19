/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.SecurityContext;

import java.util.List;

/**
 * Resolves linked and authorized projects, if running in a cross-project environment.
 * In non-cross-project environments, `resolve` returns `ResolvedProjects.VOID`
 */
public interface CrossProjectTargetResolver {
    ResolvedProjects resolve(SecurityContext securityContext);

    class Default implements CrossProjectTargetResolver {
        @Override
        public ResolvedProjects resolve(SecurityContext securityContext) {
            return ResolvedProjects.VOID;
        }
    }

    record ResolvedProjects(@Nullable String origin, List<String> projects) {
        public static ResolvedProjects VOID = new ResolvedProjects(null, List.of());

        public boolean isOriginOnly() {
            return origin != null && projects.isEmpty();
        }
    }
}
