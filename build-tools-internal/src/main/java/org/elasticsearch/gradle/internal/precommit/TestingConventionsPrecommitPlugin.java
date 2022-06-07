/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.gradle.api.Action;
import org.gradle.api.NamedDomainObjectProvider;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

import java.util.List;

public class TestingConventionsPrecommitPlugin extends PrecommitPlugin {

    public static final String TESTING_CONVENTIONS_TASK_NAME = "testingConventions";

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        project.getPlugins().apply(JavaBasePlugin.class);
        var javaPluginExtension = project.getExtensions().getByType(JavaPluginExtension.class);
        var sourceSets = javaPluginExtension.getSourceSets();
        var tasks = project.getTasks();

        project.getPlugins().withType(JavaPlugin.class, javaPlugin -> {
            NamedDomainObjectProvider<SourceSet> sourceSet = sourceSets.named(SourceSet.TEST_SOURCE_SET_NAME);
            setupTaskPerSourceSet(project, sourceSet, t -> {
                t.getSuffix().convention("Tests");
                t.getBaseClasses().convention(List.of("org.apache.lucene.tests.util.LuceneTestCase"));
            });
        });

        // Create a convenience task for all checks (this does not conflict with extension, as it has higher priority in DSL):
        return tasks.register(TESTING_CONVENTIONS_TASK_NAME, task -> {
            task.setDescription("Runs all testing conventions checks.");
            task.dependsOn(tasks.withType(TestingConventionsCheckTask.class));
        });
    }

    private void setupTaskPerSourceSet(
        Project project,
        NamedDomainObjectProvider<SourceSet> sourceSetProvider,
        Action<TestingConventionsCheckTask> config
    ) {
        sourceSetProvider.configure(sourceSet -> {
            String taskName = sourceSet.getTaskName(null, TESTING_CONVENTIONS_TASK_NAME);
            TaskProvider<TestingConventionsCheckTask> register = project.getTasks()
                .register(taskName, TestingConventionsCheckTask.class, task -> {
                    task.getTestClassesDirs().from(sourceSet.getOutput().getClassesDirs());
                    task.getClasspath().from(sourceSet.getRuntimeClasspath());
                });
            register.configure(config);
        });
    }

}
