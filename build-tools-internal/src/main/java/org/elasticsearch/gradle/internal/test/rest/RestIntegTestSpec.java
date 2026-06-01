/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest;

import org.gradle.api.tasks.testing.Test;

/**
 * Marker task extension identifying a {@link Test} task as a REST integration test.
 * <p>
 * Attached to a task via {@link #mark(Test)} once the task is enrolled in the
 * {@code restTests} project extension. The marker makes the task detectable via plain
 * Java as {@code task.getExtensions().findByType(RestIntegTestSpec.class) != null}.
 */
public class RestIntegTestSpec {

    private RestIntegTestSpec() {}

    /**
     * Attaches the {@link RestIntegTestSpec} marker extension to the given task.
     * Idempotent — safe to call more than once for the same task.
     */
    public static void mark(Test task) {
        if (task.getExtensions().findByType(RestIntegTestSpec.class) == null) {
            task.getExtensions().add(RestIntegTestSpec.class, "restIntegTestSpec", new RestIntegTestSpec());
        }
    }
}
