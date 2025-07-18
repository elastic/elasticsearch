/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

import java.util.Map;

public class AggregateTransportVersionDeclarationsPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        // need to have this task depend on all the tasks with BaseInternalPluginBuildPlugin registered
        // need to get the output of all those tasks as input to this task
        System.out.println("Potato: AggregateTransportVersionDeclarationsPlugin");
        // First thing is to create a configuration (holder for dependency information. Configurations are how the dep graph is modeled).
        var configuration = project.getConfigurations().create("aggregateTransportVersionDeclarations");
        var deps = project.getDependencies();

        project.getRootProject()
            .getSubprojects()
            .stream()
            .filter(
                p -> p.getParent().getPath().equals(":modules")
                    || p.getParent().getPath().equals(":plugins")
                    || p.getParent().getPath().equals(":x-pack:plugin")
            )
            .forEach(p -> {
                deps.add(
                    configuration.getName(),
                    deps.project(Map.of("path", p.getPath(), "configuration", "locateTransportVersionsConfig"))
                ); // adding a dep to the config we created
            });

        var aggregationTask = project.getTasks()
            .register("aggregateTransportVersionDeclarations", AggregateTransportVersionDeclarationsTask.class, t -> {
                t.dependsOn(configuration); // this task can only run after this config is resolved
                t.getTransportVersionNameDeclarationsFiles().setFrom(configuration);
                t.getOutputFile()
                    .set(project.getLayout().getBuildDirectory().file("generated-transport-info/all-transport-version-names.txt"));
            });

    }

}
