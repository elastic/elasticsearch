/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.packer;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.ResolveAllDependencies;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;

public class CacheTestFixtureResourcesPlugin implements Plugin<Project> {

    public static final String CACHE_TEST_FIXTURES = "cacheTestFixtures";

    @Override
    public void apply(Project project) {

        var cacheTestFixturesConfiguration = project.getConfigurations().create(CACHE_TEST_FIXTURES);
        cacheTestFixturesConfiguration.defaultDependencies(deps -> {
            DependencyHandler dependencyHandler = project.getDependencies();
            Dependency reflections = dependencyHandler.create(
                "org.reflections:reflections:" + VersionProperties.getVersions().get("reflections")
            );
            deps.add(reflections);
        });

        project.getPlugins().withType(JavaPlugin.class, javaPlugin -> {
            var cacheTestFixtures = project.getTasks().register(CACHE_TEST_FIXTURES, CacheCacheableTestFixtures.class, (t) -> {
                var testSourceSet = project.getExtensions()
                    .getByType(JavaPluginExtension.class)
                    .getSourceSets()
                    .getByName(JavaPlugin.TEST_TASK_NAME);
                t.getClasspath().from(cacheTestFixturesConfiguration);
                t.getClasspath().from(testSourceSet.getRuntimeClasspath());
            });
            project.getTasks().withType(ResolveAllDependencies.class).configureEach(r -> r.dependsOn(cacheTestFixtures));
        });

    }
}
