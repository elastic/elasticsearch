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

import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.setupYamlRestTestDependenciesDefaults;

/**
 * Apply this plugin to run the YAML based REST tests.
 */
public class InternalYamlRestTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "yamlRestTest";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(RestTestBasePlugin.class);
        project.getPluginManager().apply(RestResourcesPlugin.class);
        TestingExtension testing = project.getExtensions().getByType(TestingExtension.class);
        testing.getSuites().registerBinding(YamlRestTestSuite.class, DefaultYamlRestTestSuite.class);
        testing.getSuites().register(SOURCE_SET_NAME, YamlRestTestSuite.class, suite -> {
            suite.useJUnit();
            configureYamlSourceSet(project, suite.getSources());
        });
    }

    private void configureYamlSourceSet(Project project, SourceSet yamlTestSourceSet) {
        // setup the dependencies
        setupYamlRestTestDependenciesDefaults(project, yamlTestSourceSet, true);

        // setup the copy for the rest resources
        project.getTasks().withType(CopyRestApiTask.class).configureEach(copyRestApiTask -> {
            copyRestApiTask.setSourceResourceDir(
                yamlTestSourceSet.getResources()
                    .getSrcDirs()
                    .stream()
                    .filter(f -> f.isDirectory() && f.getName().equals("resources"))
                    .findFirst()
                    .orElse(null)
            );
        });

        // Register rest resources with source set
        yamlTestSourceSet.getOutput()
            .dir(
                project.getTasks()
                    .withType(CopyRestApiTask.class)
                    .named(RestResourcesPlugin.COPY_REST_API_SPECS_TASK)
                    .flatMap(CopyRestApiTask::getOutputResourceDir)
            );

        yamlTestSourceSet.getOutput()
            .dir(
                project.getTasks()
                    .withType(CopyRestTestsTask.class)
                    .named(RestResourcesPlugin.COPY_YAML_TESTS_TASK)
                    .flatMap(CopyRestTestsTask::getOutputResourceDir)
            );

        GradleUtils.setupIdeForTestSourceSet(project, yamlTestSourceSet);
    }
}
