/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.internal.precommit.CheckForbiddenApisTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * Enables forbidden-API checking for the {@code java.lang.foreign} API so that direct use of the
 * standard-but-renamed methods (e.g. {@code MemorySegment#getString}) is caught at build time and
 * routed through the {@code MemorySegmentAdapter} helpers instead.
 *
 * <p>The Foreign Function &amp; Memory API is standard since JDK 22 and the Elasticsearch baseline
 * is JDK 25, so no {@code --enable-preview}, stub JAR or {@code --patch-module} handling is needed.
 * (The JDK 21 preview machinery that this plugin previously carried was removed with the JDK 25
 * baseline.)
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
        project.getTasks().withType(CheckForbiddenApisTask.class).configureEach(CheckForbiddenApisTask::checkForeignApiUsage);
    }
}
