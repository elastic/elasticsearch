/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.docker

import org.elasticsearch.gradle.Architecture
import org.elasticsearch.gradle.fixtures.AbstractProjectBuilderPluginSpec
import org.elasticsearch.gradle.internal.ResolveAllDependencies
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlugin
import spock.lang.TempDir

class DockerImageCachePluginSpec extends AbstractProjectBuilderPluginSpec {

    private static final String HOST_PLATFORM = Architecture.current().dockerPlatform
    private static final String FOREIGN_PLATFORM = Architecture.values().find { it != Architecture.current() }.dockerPlatform

    @TempDir
    File workspace

    Project project

    @Override
    Class<DockerImageCachePlugin> getPluginClassUnderTest() {
        return DockerImageCachePlugin
    }

    def setup() {
        writeMinimalElasticsearchRepoLayout(workspace)
        Project rootProject = buildProject("elasticsearch", null, workspace)
        // mirrors settings.gradle, which applies elasticsearch.global-build-info to the root project of the build
        rootProject.pluginManager.apply(GlobalBuildInfoPlugin)
        project = buildProject("consumer", rootProject)
    }

    def "collects native-image builder and docker base images for the host platform"() {
        given:
        applyPluginUnderTest(project)
        project.tasks.register("nativeHost", NativeImageBuildTask) {
            imageTag = "builder:host"
            platform = HOST_PLATFORM
        }
        project.tasks.register("nativeForeign", NativeImageBuildTask) {
            imageTag = "builder:foreign"
            platform = FOREIGN_PLATFORM
        }
        project.tasks.register("dockerHost", DockerBuildTask) {
            baseImages = ["base:host", "base:pinned@sha256:abc"] as String[]
            platforms.add(HOST_PLATFORM)
        }
        project.tasks.register("dockerForeign", DockerBuildTask) {
            baseImages = ["base:foreign"] as String[]
            platforms.add(FOREIGN_PLATFORM)
        }
        project.tasks.register("dockerNoPull", DockerBuildTask) {
            baseImages = ["base:nopull"] as String[]
            platforms.add(HOST_PLATFORM)
            pull = false
        }

        when:
        CacheDockerImages task = project.tasks.named(DockerImageCachePlugin.CACHE_DOCKER_IMAGES, CacheDockerImages).get()

        then:
        task.platform.get() == HOST_PLATFORM
        task.images.get() == ["builder:host", "base:host", "base:pinned@sha256:abc"] as Set
    }

    def "resolveAllDependencies depends on docker image and test fixture caching"() {
        given:
        project.pluginManager.apply(JavaPlugin)
        applyPluginUnderTest(project)
        def resolveAll = project.tasks.register("resolveAllDependencies", ResolveAllDependencies) {
            configs = []
        }

        when:
        Set<String> dependencies = resolveAll.get().taskDependencies.getDependencies(resolveAll.get())*.name as Set

        then:
        dependencies.containsAll([DockerImageCachePlugin.CACHE_DOCKER_IMAGES, DockerImageCachePlugin.CACHE_TEST_FIXTURES])
    }

    def "does not register test fixture caching for non java projects"() {
        when:
        applyPluginUnderTest(project)

        then:
        project.tasks.findByName(DockerImageCachePlugin.CACHE_TEST_FIXTURES) == null
        project.tasks.findByName(DockerImageCachePlugin.CACHE_DOCKER_IMAGES) != null
    }
}
