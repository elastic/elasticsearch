/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.internal.BuildPlugin;
import org.elasticsearch.gradle.internal.InternalTestClustersPlugin;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;

import java.util.Arrays;
import java.util.List;

/**
 * Adds support for starting an Elasticsearch cluster before running integration
 * tests. Used in conjunction with {@link StandaloneRestTestPlugin} for qa
 * projects and in conjunction with {@link BuildPlugin} for testing the rest
 * client.
 */
public class RestTestPlugin implements Plugin<Project> {
    private final List<String> REQUIRED_PLUGINS = Arrays.asList("elasticsearch.build", "elasticsearch.standalone-rest-test");

    @Override
    public void apply(final Project project) {
        if (REQUIRED_PLUGINS.stream().noneMatch(requiredPlugin -> project.getPluginManager().hasPlugin(requiredPlugin))) {
            throw new InvalidUserDataException(
                "elasticsearch.rest-test " + "requires either elasticsearch.build or " + "elasticsearch.standalone-rest-test"
            );
        }
        project.getPlugins().apply(RestTestBasePlugin.class);
        project.getPluginManager().apply(InternalTestClustersPlugin.class);
        final var integTest = project.getTasks().register("integTest", RestIntegTestTask.class, task -> {
            task.setDescription("Runs rest tests against an elasticsearch cluster.");
            task.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            task.mustRunAfter(project.getTasks().named("precommit"));
        });
        project.getTasks().named("check").configure(task -> task.dependsOn(integTest));
    }
}
