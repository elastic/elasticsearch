/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest;

import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.registerTestTask;
import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.setupJavaRestTestDependenciesDefaults;

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

        project.getDependencies().add(javaTestSourceSet.getImplementationConfigurationName(), project.project(":test:test-clusters"));

        // setup the javaRestTest task
        // we use a StandloneRestIntegTestTask here so that the conventions of RestTestBasePlugin don't create a test cluster
        TaskProvider<StandaloneRestIntegTestTask> testTask = registerTestTask(
            project,
            javaTestSourceSet,
            SOURCE_SET_NAME,
            StandaloneRestIntegTestTask.class
        );

        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(testTask));

        // setup dependencies
        setupJavaRestTestDependenciesDefaults(project, javaTestSourceSet);

        // setup IDE
        GradleUtils.setupIdeForTestSourceSet(project, javaTestSourceSet);
    }
}
