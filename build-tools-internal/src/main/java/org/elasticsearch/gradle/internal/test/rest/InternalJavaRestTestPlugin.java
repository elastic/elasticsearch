/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest;

import org.elasticsearch.gradle.internal.test.RestIntegTestTask;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.registerTestTask;
import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.setupJavaRestTestDependenciesDefaults;

import com.gradle.enterprise.gradleplugin.testdistribution.TestDistributionExtension;

/**
 * Apply this plugin to run the Java based REST tests.
 */
public class InternalJavaRestTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "javaRestTest";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(RestTestBasePlugin.class);

        // create source set
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet javaTestSourceSet = sourceSets.create(SOURCE_SET_NAME);

        if (project.findProject(":test:test-clusters") != null) {
            project.getDependencies().add(javaTestSourceSet.getImplementationConfigurationName(), project.project(":test:test-clusters"));
        }

        // setup the javaRestTest task
        TaskProvider<RestIntegTestTask> testTask = registerTestTask(project, javaTestSourceSet, SOURCE_SET_NAME, RestIntegTestTask.class);
        testTask.get().useJUnitPlatform();
        testTask.get().getExtensions().configure(TestDistributionExtension.class, config -> {
            config.getEnabled().set(true);
            config.getMaxLocalExecutors().set(0);
        });

        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(testTask));

        // setup dependencies
        setupJavaRestTestDependenciesDefaults(project, javaTestSourceSet);

        // setup IDE
        GradleUtils.setupIdeForTestSourceSet(project, javaTestSourceSet);
    }
}
