/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import spock.lang.Specification
import spock.lang.TempDir

import org.gradle.api.Project
import org.gradle.api.internal.TaskInputsInternal
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.testing.Test
import org.gradle.internal.fingerprint.DirectorySensitivity
import org.gradle.internal.fingerprint.FileNormalizer
import org.gradle.internal.fingerprint.LineEndingSensitivity
import org.gradle.internal.properties.InputBehavior
import org.gradle.internal.properties.InputFilePropertyType
import org.gradle.internal.properties.PropertyValue
import org.gradle.internal.properties.PropertyVisitor
import org.gradle.testfixtures.ProjectBuilder

/**
 * Unit coverage for the cache-relocatable input wiring in {@link ElasticsearchTestBasePlugin}.
 * <p>
 * The plugin registers the immutable-collections patch and the entitlement agent/bridge jars as
 * task inputs. Historically these were anonymous, absolute-path-sensitive file inputs, which pinned
 * the build cache key to the checkout location and to the jar manifest's build timestamp, so test
 * tasks never got a cache hit. These tests lock in the fix: every such input carries a stable
 * property name and an appropriate normalizer ({@code NAME_ONLY} for the patch dir, classpath
 * normalization for the jars so the manifest timestamp is ignored), and the bridge jar is declared
 * as an input exactly once (via {@code entitlementBridgeJavaBasePatch}) rather than twice.
 */
class ElasticsearchTestBasePluginSpec extends Specification {

    @TempDir
    File workspace

    Project rootProject

    def setup() {
        // elasticsearch.test-base bootstraps GlobalBuildInfoPlugin, which resolves the workspace to the
        // root project dir and reads <workspace>/build-tools-internal/version.properties. Provide a minimal
        // one so the plugin can be applied in isolation without the full repository layout.
        new File(workspace, "build-tools-internal").mkdirs()
        new File(workspace, "build-tools-internal/version.properties").text = """
            elasticsearch      = 9.1.0
            lucene             = 10.2.2
            bundled_jdk_vendor = openjdk
            bundled_jdk        = 24+36@1f9ff9062db4449d8ca828c504ffae90
            minimumJdkVersion  = 21
            minimumRuntimeJava = 21
            minimumCompilerJava = 21
        """.stripIndent()

        rootProject = ProjectBuilder.builder()
            .withProjectDir(workspace)
            .withName("elasticsearch")
            .build()

        // The immutable-collections patch input wires a dependency on the "patch" configuration of
        // :test:immutable-collections-patch, so that project must exist for the input to be registered.
        def testProject = ProjectBuilder.builder().withParent(rootProject).withName("test").build()
        def patchProject = ProjectBuilder.builder().withParent(testProject).withName("immutable-collections-patch").build()
        patchProject.configurations.create("patch") {
            it.canBeConsumed = true
            it.canBeResolved = false
        }

        rootProject.pluginManager.apply(JavaPlugin)
        rootProject.pluginManager.apply(ElasticsearchTestBasePlugin)
        // internalClusterTest is one of TEST_TASKS_WITH_ENTITLEMENTS but is not created by the java plugin.
        rootProject.tasks.register("internalClusterTest", Test) {
            it.testClassesDirs = rootProject.files()
            it.classpath = rootProject.files()
        }
    }

    def "test task entitlement and patch inputs are named and normalized for cache relocatability"() {
        when:
        def inputs = inputFileProperties((Test) rootProject.tasks.getByName(JavaPlugin.TEST_TASK_NAME))

        then:
        // NAME_ONLY: the patch content keys the cache, but the (per-checkout) absolute path must not.
        inputs["patchedImmutableCollections"]?.normalizer == "NAME_ONLY"
        inputs["patchedImmutableCollections"]?.optional == false

        and:
        // Classpath normalization so the jar manifest (which embeds a build timestamp) is ignored.
        inputs["entitlementBridgeJavaBasePatch"]?.normalizer == "RUNTIME_CLASSPATH"
        inputs["entitlementAgent"]?.normalizer == "RUNTIME_CLASSPATH"

        and:
        // The agent jar is absent in projects without entitlements, hence optional.
        inputs["entitlementAgent"]?.optional == true

        and:
        // The bridge jar is patched into java.base and is tracked as an input exactly once, under
        // entitlementBridgeJavaBasePatch. It must not also be registered under a second property name.
        inputs.containsKey("entitlementBridge") == false
    }

    def "internalClusterTest gets the entitlement inputs but not the immutable-collections patch"() {
        when:
        def inputs = inputFileProperties((Test) rootProject.tasks.getByName("internalClusterTest"))

        then:
        inputs["entitlementBridgeJavaBasePatch"]?.normalizer == "RUNTIME_CLASSPATH"
        inputs["entitlementAgent"]?.normalizer == "RUNTIME_CLASSPATH"

        and:
        // The bridge is tracked once (entitlementBridgeJavaBasePatch), not under a second property name.
        inputs.containsKey("entitlementBridge") == false

        and:
        // The immutable-collections patch is wired only for the "test" task.
        inputs.containsKey("patchedImmutableCollections") == false
    }

    /**
     * Collects the registered input file properties of a task as {@code name -> [normalizer, optional]}.
     * The normalizer is reported by its {@link org.gradle.internal.execution.model.InputNormalizer} enum
     * constant name (e.g. {@code NAME_ONLY}, {@code RUNTIME_CLASSPATH}).
     */
    private static Map<String, Map> inputFileProperties(Test task) {
        Map<String, Map> collected = [:]
        ((TaskInputsInternal) task.inputs).visitRegisteredProperties(new PropertyVisitor() {
            @Override
            void visitInputFileProperty(
                String propertyName,
                boolean optional,
                InputBehavior behavior,
                DirectorySensitivity directorySensitivity,
                LineEndingSensitivity lineEndingSensitivity,
                FileNormalizer normalizer,
                PropertyValue value,
                InputFilePropertyType filePropertyType
            ) {
                collected[propertyName] = [normalizer: ((Enum) normalizer).name(), optional: optional]
            }
        })
        return collected
    }
}
