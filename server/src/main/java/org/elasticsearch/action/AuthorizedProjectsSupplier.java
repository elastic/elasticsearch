/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.core.Nullable;

import java.util.List;

/**
 * Returns authorized projects (linked and origin), if running in a cross-project environment.
 * In non-cross-project environments, returns a special value indicating that cross-project does not apply at all.
 */
public interface AuthorizedProjectsSupplier {
    AuthorizedProjects get();

    class Default implements AuthorizedProjectsSupplier {
        @Override
        public AuthorizedProjects get() {
            return AuthorizedProjects.NOT_CROSS_PROJECT;
        }
    }

    record AuthorizedProjects(@Nullable String origin, List<String> projects) {
        public static AuthorizedProjects NOT_CROSS_PROJECT = new AuthorizedProjects(null, List.of());

        public boolean isOriginOnly() {
            return origin != null && projects.isEmpty();
        }
    }
}
