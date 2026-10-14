/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.nativelibs

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin

/**
 * The collecting project never names a producer: a project is picked up because it applied
 * {@link NativeLibraryBuildPlugin}, which is the whole point of the topic.
 *
 * <p>These run a real build in order to exercise the real situation, in which the topic fills up
 * only as each producer is configured. Here the collector is the root project, which Gradle
 * evaluates before any subproject, so the topic is still empty when the plugin is applied: a
 * collector that read it eagerly would gather nothing, and these tests would catch it.
 */
class NativeLibraryCollectionPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    /** The platform directory a build has to populate on this machine. */
    static final String PLATFORM = BuildNativeLibraryTask.hostPlatform()

    @Override
    <T extends Plugin> Class<T> getPluginClassUnderTest() {
        NativeLibraryCollectionPlugin.class
    }

    def setup() {
        buildFile << """
        import org.gradle.api.artifacts.type.ArtifactTypeDefinition

        configurations.resolvable('collected') {
          extendsFrom(configurations.${NativeLibraryCollectionPlugin.PROJECTS_CONFIGURATION})
          attributes.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
        }
        tasks.register('collect', Copy) {
          from configurations.collected
          into layout.buildDirectory.dir('platform')
        }
        """
    }

    def "collects the library of every project that applied the build plugin"() {
        given:
        nativeLibraryProject('alpha')
        nativeLibraryProject('beta')

        when:
        gradleRunner("collect").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        collected('alpha').exists()
        collected('beta').exists()
    }

    /**
     * A project that builds a native library named after itself, so a collected tree can be told
     * apart from another project's. Applies the plugin and declares nothing else about collection.
     */
    private void nativeLibraryProject(String name) {
        settingsFile << """
        include '${name}'
        """
        file("${name}/native/Makefile") << "all:\n\ttrue\n"
        file("${name}/build.gradle") << """
        import ${NativeLibraryBuildPlugin.name}

        plugins.apply(NativeLibraryBuildPlugin)

        nativeLibraryBuild {
          modeEnvironmentVariable = 'TEST_NATIVE_BUILD'
          supportedPlatforms = ['${PLATFORM}']
          workingDir = layout.projectDirectory.dir('native')
          sources = ['native/Makefile']
          toolchainImage = 'example/toolchain:1'
          dockerCommand = ['make', 'all']
          hostCommand { outputDir -> ['sh', '-c', "mkdir -p \$outputDir.asFile/${PLATFORM} && echo built > \$outputDir.asFile/${PLATFORM}/lib${name}.so"] }
        }
        """
    }

    /** Where the collecting project leaves the library built by {@code name}. */
    private File collected(String name) {
        file("build/platform/${PLATFORM}/lib${name}.so")
    }
}
