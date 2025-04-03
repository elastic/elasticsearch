/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.precommit;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

public class PrecommitTaskPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        TaskProvider<Task> precommit = project.getTasks().register(PrecommitPlugin.PRECOMMIT_TASK_NAME, t -> {
            t.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            t.setDescription("Runs all non-test checks");
        });

        project.getPluginManager()
                .withPlugin(
                        "lifecycle-base",
                        p -> project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(t -> t.dependsOn(precommit))
                );
        project.getPluginManager().withPlugin("java-base", p -> {
            // run compilation as part of precommit
            project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets().configureEach(sourceSet ->
                    precommit.configure(t -> t.dependsOn(sourceSet.getClassesTaskName()))
            );
            // make sure tests run after all precommit tasks
            project.getTasks().withType(Test.class).configureEach(t -> t.mustRunAfter(precommit));
        });
    }
}
