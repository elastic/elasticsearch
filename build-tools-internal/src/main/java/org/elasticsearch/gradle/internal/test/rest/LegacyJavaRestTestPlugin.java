/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest;

import org.elasticsearch.gradle.internal.test.LegacyRestTestBasePlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;

import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.registerTestTask;
import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.setupJavaRestTestDependenciesDefaults;

/**
 * Apply this plugin to run the Java based REST tests.
 *
 * @deprecated use {@link InternalJavaRestTestPlugin}
 */
@Deprecated
public class LegacyJavaRestTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "javaRestTest";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(LegacyRestTestBasePlugin.class);

        // create source set
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet javaTestSourceSet = sourceSets.create(SOURCE_SET_NAME);

        // setup the javaRestTest task
        registerTestTask(project, javaTestSourceSet);

        // setup dependencies
        setupJavaRestTestDependenciesDefaults(project, javaTestSourceSet);

        // setup IDE
        GradleUtils.setupIdeForTestSourceSet(project, javaTestSourceSet);
    }
}
