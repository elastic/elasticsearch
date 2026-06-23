/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * Configures a project to use the {@code java.lang.foreign} API without
 * {@code --enable-preview} on JDK 21. On JDK 22+ the Foreign Function and
 * Memory API is standard, so this is effectively a no-op.
 *
 * <p>Apply in a project's {@code build.gradle}:
 * <pre>{@code
 *   apply plugin: 'elasticsearch.foreign-api'
 * }</pre>
 */
public class ForeignApiPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);
        ElasticsearchJavaBasePlugin.enableForeignAccess(project);
    }
}
