/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.dsl.DependencyCollector;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JvmTestSuitePlugin;
import org.gradle.api.plugins.jvm.JvmTestSuite;
import org.gradle.api.tasks.SourceSet;
import org.gradle.testing.base.TestingExtension;

public class InternalClusterTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "internalClusterTest";

    @Override
    public void apply(Project project) {
        project.getRootProject().getPlugins().apply(GlobalBuildInfoPlugin.class);
        project.getPluginManager().apply(JavaPlugin.class);
        project.getPluginManager().apply(JvmTestSuitePlugin.class);
        TestingExtension testing = project.getExtensions().getByType(TestingExtension.class);

        testing.getSuites().register(SOURCE_SET_NAME, JvmTestSuite.class, suite -> {
            suite.useJUnit();
            DependencyCollector implementation = suite.getDependencies().getImplementation();
            implementation.add(suite.getDependencies().project());
            suite.getTargets().configureEach(target -> { target.getTestTask().configure(test -> { test.jvmArgs("-XX:+UseG1GC"); }); });
        });

        project.getPluginManager().withPlugin("java", plugin -> {
            // TODO: fix usages of IT tests depending on Tests methods so this extension is not necessary
            GradleUtils.extendSourceSet(project, SourceSet.TEST_SOURCE_SET_NAME, SOURCE_SET_NAME);
        });
    }
}
