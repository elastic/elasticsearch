/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.precommit;

import org.gradle.api.Project;

/**
 * Validation tasks which should be run before committing. These run before tests.
 */
public class PrecommitTasks {
    /**
     * Adds a precommit task, which depends on non-test verification tasks.
     */
    public static void create(Project project) {
        project.getPluginManager().apply(JarHellPrecommitPlugin.class);
    }
}
