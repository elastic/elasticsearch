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
import org.gradle.api.tasks.SourceSet;
import org.gradle.testing.base.TestingExtension;

import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.setupJavaRestTestDependenciesDefaults;

/**
 * Apply this plugin to run the Java based REST tests.
 */
public class InternalJavaRestTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "javaRestTest";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(RestTestBasePlugin.class);
        TestingExtension testing = project.getExtensions().getByType(TestingExtension.class);
        testing.getSuites().registerBinding(JavaRestTestSuite.class, DefaultJavaRestTestSuite.class);
        testing.getSuites().register(SOURCE_SET_NAME, JavaRestTestSuite.class, suite -> {
            suite.useJUnit();
            configureJavaRestSources(project, suite.getSources());
            if (project.findProject(":test:test-clusters") != null) {
                suite.getDependencies().getImplementation().add(suite.getDependencies().project(":test:test-clusters"));
            }
        });

    }

    private void configureJavaRestSources(Project project, SourceSet javaTestSourceSet) {
        // setup dependencies
        setupJavaRestTestDependenciesDefaults(project, javaTestSourceSet);

        // setup IDE
        GradleUtils.setupIdeForTestSourceSet(project, javaTestSourceSet);
    }

}
