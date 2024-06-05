/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.internal.ElasticsearchJavaBasePlugin;
import org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.precommit.InternalPrecommitTasks;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

/**
 * Configures the build to compile against Elasticsearch's test framework and
 * run integration and unit tests. Use BuildPlugin if you want to build main
 * code as well as tests.
 */
public class StandaloneTestPlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);
        project.getPluginManager().apply(ElasticsearchTestBasePlugin.class);

        // only setup tests to build
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        final SourceSet testSourceSet = sourceSets.create("test");
        project.getDependencies().add(testSourceSet.getImplementationConfigurationName(), project.project(":test:framework"));

        project.getTasks().withType(Test.class).configureEach(test -> {
            test.setTestClassesDirs(testSourceSet.getOutput().getClassesDirs());
            test.setClasspath(testSourceSet.getRuntimeClasspath());
        });
        TaskProvider<Test> testTask = project.getTasks().register("test", Test.class);
        testTask.configure(test -> {
            test.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            test.setDescription("Runs unit tests that are separate");
            test.mustRunAfter(project.getTasks().getByName("precommit"));
        });

        project.getTasks().named("check").configure(task -> task.dependsOn(testTask));

        InternalPrecommitTasks.create(project, false);
    }
}
