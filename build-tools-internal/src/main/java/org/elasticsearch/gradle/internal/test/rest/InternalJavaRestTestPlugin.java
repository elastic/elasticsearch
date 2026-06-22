/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest;

import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.registerPlainRestTestTask;
import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.setupJavaRestTestDependenciesDefaults;

/**
 * Apply this plugin to run the Java based REST tests.
 * <p>
 * The registered {@code javaRestTest} task is a plain {@link Test} task (not backed by the Gradle
 * test-cluster framework). Runtime cluster management is handled by the JUnit-rule based framework
 * in {@code :test:test-clusters}. The task is enrolled in the {@code restTests} project extension
 * so that standard REST integ-test configuration (distribution wiring, system properties, etc.) is
 * applied automatically by {@link RestTestBasePlugin}.
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

        // setup the javaRestTest task as a plain Test task enrolled in restTests
        TaskProvider<Test> testTask = registerPlainRestTestTask(project, javaTestSourceSet, SOURCE_SET_NAME);

        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(testTask));

        // setup dependencies
        setupJavaRestTestDependenciesDefaults(project, javaTestSourceSet);

        // setup IDE
        GradleUtils.setupIdeForTestSourceSet(project, javaTestSourceSet);
    }
}
