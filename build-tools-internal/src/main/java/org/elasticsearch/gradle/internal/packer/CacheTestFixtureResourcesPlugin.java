/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.packer;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.ResolveAllDependencies;
import org.elasticsearch.gradle.internal.docker.DockerSupportPlugin;
import org.elasticsearch.gradle.internal.docker.NativeImageBuildTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Caches external resources required by a project's build and tests as part of {@code resolveAllDependencies}, which is
 * run when baking CI agent images (see {@code .buildkite/packer_cache.sh}). This avoids pulling them from remote
 * registries at job time. Covers:
 * <ul>
 *     <li>resources (usually Docker images) of {@code CacheableTestFixture} implementations in this project</li>
 *     <li>the Docker builder images used by this project's {@link NativeImageBuildTask}s</li>
 * </ul>
 */
public class CacheTestFixtureResourcesPlugin implements Plugin<Project> {

    public static final String CACHE_TEST_FIXTURES = "cacheTestFixtures";
    public static final String CACHE_NATIVE_IMAGE_BUILDER = "cacheNativeImageBuilder";

    @Override
    public void apply(Project project) {
        configureTestFixtureCaching(project);
        configureNativeImageBuilderCaching(project);
    }

    private static void configureTestFixtureCaching(Project project) {
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
                // Test fixtures live in the main source set of fixture projects. Using the main rather than the test
                // runtime classpath avoids compiling test code (and e.g. the test framework) when baking agent images.
                var mainSourceSet = project.getExtensions()
                    .getByType(JavaPluginExtension.class)
                    .getSourceSets()
                    .getByName(SourceSet.MAIN_SOURCE_SET_NAME);
                t.getClasspath().from(cacheTestFixturesConfiguration);
                t.getClasspath().from(mainSourceSet.getRuntimeClasspath());
            });
            project.getTasks().withType(ResolveAllDependencies.class).configureEach(r -> r.dependsOn(cacheTestFixtures));
        });
    }

    /**
     * Only images for the host architecture are cached: CI agents only build native images for their own architecture,
     * and the Docker image store holds a single platform variant per tag.
     */
    private static void configureNativeImageBuilderCaching(Project project) {
        project.getPluginManager().apply(DockerSupportPlugin.class);
        String hostPlatform = Architecture.current().dockerPlatform;
        var cacheNativeImageBuilder = project.getTasks().register(CACHE_NATIVE_IMAGE_BUILDER, CacheDockerImages.class, t -> {
            t.setDescription("Pulls the Docker images used by native-image builds for the host architecture");
            t.getPlatform().set(hostPlatform);
            t.getImages()
                .set(
                    project.provider(
                        () -> project.getTasks()
                            .withType(NativeImageBuildTask.class)
                            .stream()
                            .filter(n -> hostPlatform.equals(n.getPlatform().getOrNull()))
                            .map(n -> n.getImageTag().getOrNull())
                            .filter(Objects::nonNull)
                            .collect(Collectors.toSet())
                    )
                );
        });
        project.getTasks().withType(ResolveAllDependencies.class).configureEach(r -> r.dependsOn(cacheNativeImageBuilder));
    }
}
