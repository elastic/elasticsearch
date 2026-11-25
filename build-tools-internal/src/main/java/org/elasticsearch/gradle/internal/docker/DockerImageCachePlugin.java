/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.docker;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.ResolveAllDependencies;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Pulls Docker images required by a project's build and tests as part of {@code resolveAllDependencies}, which is run
 * when baking CI agent images (see {@code .buildkite/packer_cache.sh}). This avoids pulling them from remote registries
 * at job time. Covers:
 * <ul>
 *     <li>images of {@code CacheableTestFixture} implementations (test containers) in this project</li>
 *     <li>the builder images used by this project's {@link NativeImageBuildTask}s</li>
 *     <li>the base images declared by this project's {@link DockerBuildTask}s</li>
 * </ul>
 */
public class DockerImageCachePlugin implements Plugin<Project> {

    public static final String CACHE_TEST_FIXTURES = "cacheTestFixtures";
    public static final String CACHE_DOCKER_IMAGES = "cacheDockerImages";

    @Override
    public void apply(Project project) {
        configureTestFixtureCaching(project);
        configureDockerImageCaching(project);
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
     * Only images for the host architecture are cached: CI agents only build images for their own architecture (cross
     * architecture builds use buildx, which has its own image store), and the Docker image store holds a single platform
     * variant per tag.
     */
    private static void configureDockerImageCaching(Project project) {
        project.getPluginManager().apply(DockerSupportPlugin.class);
        String hostPlatform = Architecture.current().dockerPlatform;
        var cacheDockerImages = project.getTasks().register(CACHE_DOCKER_IMAGES, CacheDockerImages.class, t -> {
            t.setDescription("Pulls the Docker base and builder images used by this project for the host architecture");
            t.getPlatform().set(hostPlatform);
            t.getImages().set(project.provider(() -> {
                Set<String> images = new LinkedHashSet<>();
                project.getTasks().withType(NativeImageBuildTask.class).forEach(n -> {
                    if (hostPlatform.equals(n.getPlatform().getOrNull()) && n.getImageTag().isPresent()) {
                        images.add(n.getImageTag().get());
                    }
                });
                project.getTasks().withType(DockerBuildTask.class).forEach(d -> {
                    if (d.isPull() && d.getBaseImages() != null && d.getPlatforms().getOrElse(Set.of()).contains(hostPlatform)) {
                        images.addAll(Arrays.asList(d.getBaseImages()));
                    }
                });
                return images;
            }));
        });
        project.getTasks().withType(ResolveAllDependencies.class).configureEach(r -> r.dependsOn(cacheDockerImages));
    }
}
