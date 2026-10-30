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

    def "collects the library of every project that applied the build plugin under isolated projects"() {
        given:
        nativeLibraryProject('alpha')
        nativeLibraryProject('beta')

        when:
        gradleRunner(":collect", "--isolated-projects").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        collected('alpha').exists()
        collected('beta').exists()

        when:
        collected('alpha').delete()
        collected('beta').delete()
        def warm = gradleRunner(":collect", "--isolated-projects").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        warm.output.contains("Configuration cache entry reused")
        collected('alpha').exists()
        collected('beta').exists()

        when:
        file("alpha/build.gradle") << "\ntasks.register('unrelated')\n"
        collected('alpha').delete()
        collected('beta').delete()
        gradleRunner(":collect", "--isolated-projects").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        collected('alpha').exists()
        collected('beta').exists()
    }

    def "ignores a project that does not apply the build plugin, even if it has a native library"() {
        given:
        nativeLibraryProject('alpha')
        lookalikeProject('gamma')

        when:
        gradleRunner("collect").withEnvironment(["TEST_NATIVE_BUILD": "host"]).build()

        then:
        collected('alpha').exists()
        collected('gamma').exists() == false
    }

    /**
     * A project that looks like a producer (same consumable configuration, same library layout)
     * but never applies the plugin.
     */
    private void lookalikeProject(String name) {
        settingsFile << """
        include '${name}'
        """
        file("${name}/prebuilt/${PLATFORM}/lib${name}.so") << "built\n"
        file("${name}/build.gradle") << """
        import org.gradle.api.artifacts.type.ArtifactTypeDefinition

        configurations.consumable('${NativeLibraryBuildPlugin.ELEMENTS_CONFIGURATION}') {
          attributes.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
        }
        artifacts.add('${NativeLibraryBuildPlugin.ELEMENTS_CONFIGURATION}', layout.projectDirectory.dir('prebuilt'))
        """
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
