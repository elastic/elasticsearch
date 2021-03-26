/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest;

import org.elasticsearch.gradle.ElasticsearchJavaPlugin;
import org.elasticsearch.gradle.test.RestIntegTestTask;
import org.elasticsearch.gradle.test.RestTestBasePlugin;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;

import static org.elasticsearch.gradle.test.rest.RestTestUtil.createTestCluster;
import static org.elasticsearch.gradle.test.rest.RestTestUtil.registerTask;
import static org.elasticsearch.gradle.test.rest.RestTestUtil.setupDependencies;

/**
 * Apply this plugin to run the Java based REST tests.
 */
public class JavaRestTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "javaRestTest";

    @Override
    public void apply(Project project) {

        project.getPluginManager().apply(ElasticsearchJavaPlugin.class);
        project.getPluginManager().apply(RestTestBasePlugin.class);
        project.getPluginManager().apply(TestClustersPlugin.class);

        // create source set
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet javaTestSourceSet = sourceSets.create(SOURCE_SET_NAME);

        // create the test cluster container
        createTestCluster(project, javaTestSourceSet);

        // setup the javaRestTest task
        Provider<RestIntegTestTask> javaRestTestTask = registerTask(project, javaTestSourceSet);

        // setup dependencies
        setupDependencies(project, javaTestSourceSet);

        // setup IDE
        GradleUtils.setupIdeForTestSourceSet(project, javaTestSourceSet);

        // wire this task into check
        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(javaRestTestTask));
    }
}
