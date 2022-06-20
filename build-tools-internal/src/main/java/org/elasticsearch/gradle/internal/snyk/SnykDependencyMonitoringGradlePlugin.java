/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.snyk;

import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class SnykDependencyMonitoringGradlePlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        var task = project.getTasks().register("resolveSnykDependencyGraph", GenerateSnykDependencyGraph.class);
        project.getTasks().register("uploadSnykDependencyGraph");
        var config = project.getConfigurations().create("someConfig");
        task.configure(new Action<GenerateSnykDependencyGraph>() {
            @Override
            public void execute(GenerateSnykDependencyGraph generateSnykDependencyGraph) {
            generateSnykDependencyGraph.setConfiguration(config);
            }
        });
    }

}
