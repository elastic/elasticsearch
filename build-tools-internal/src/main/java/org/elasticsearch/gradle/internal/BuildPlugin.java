/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.precommit.InternalPrecommitTasks;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * Encapsulates build configuration for elasticsearch projects.
 */
public class BuildPlugin implements Plugin<Project> {

    @Override
    public void apply(final Project project) {
        // make sure the global build info plugin is applied to the root project
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);

        project.getRootProject().getPlugins().apply(ElasticsearchBasePlugin.class);
        if (project.getPluginManager().hasPlugin("elasticsearch.standalone-rest-test")) {
            throw new InvalidUserDataException(
                "elasticsearch.standalone-test, " + "elasticsearch.standalone-rest-test, and elasticsearch.build are mutually exclusive"
            );
        }

        project.getPluginManager().apply("elasticsearch.java");
        project.getPluginManager().apply("elasticsearch.publish");
        project.getPluginManager().apply(DependenciesInfoPlugin.class);
        project.getPluginManager().apply(DependenciesGraphPlugin.class);
        InternalPrecommitTasks.create(project, true);
    }

}
