/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public License
 * v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest

import org.elasticsearch.gradle.fixtures.AbstractProjectBuilderPluginSpec
import spock.lang.TempDir

import org.gradle.api.Project
import org.gradle.api.internal.TaskInputsInternal
import org.gradle.api.plugins.JavaPlugin
import org.gradle.internal.fingerprint.DirectorySensitivity
import org.gradle.internal.fingerprint.FileNormalizer
import org.gradle.internal.fingerprint.LineEndingSensitivity
import org.gradle.internal.properties.InputBehavior
import org.gradle.internal.properties.InputFilePropertyType
import org.gradle.internal.properties.PropertyValue
import org.gradle.internal.properties.PropertyVisitor

/**
 * Unit coverage for the cache-relocatable input wiring in {@link RestTestBasePlugin} for tasks that
 * opt into {@code usesDefaultDistribution(...)}.
 * <p>
 * When a test task opts in, the plugin registers the default distribution's cluster-features metadata
 * config as a task input. Historically this used {@code PathSensitivity.NONE} (anonymous, byte-level
 * sensitivity) which pinned the build cache key to absolute paths and to every byte of the metadata
 * file. The fix names the input ({@code defaultDistroFeatureMetadata}) and applies classpath
 * normalization so the cache key is stable across checkouts and across feature-set-only changes,
 * letting tasks like {@code :x-pack:plugin:sql:qa:server:security:with-ssl:javaRestTest} that
 * genuinely need the default distribution still hit the remote build cache.
 */
class RestTestBasePluginSpec extends AbstractProjectBuilderPluginSpec {

    @Override
    Class<InternalJavaRestTestPlugin> getPluginClassUnderTest() {
        return InternalJavaRestTestPlugin
    }

    @TempDir
    File workspace

    Project rootProject

    def setup() {
        writeMinimalElasticsearchRepoLayout(workspace)
        // The Test task in InternalJavaRestTestPlugin triggers MutedTestsBuildService, which reads
        // <settingsDir>/muted-tests.yml. Provide an empty stub so the service can instantiate in
        // isolation without the full repository layout.
        new File(workspace, "muted-tests.yml").text = "tests: []\n"

        rootProject = buildProject("elasticsearch", null, workspace)
        // RestTestBasePlugin's usesDefaultDistribution closure also touches the BWC versions loader
        // and DistributionDownloadPlugin; the minimal layout is sufficient because we don't trigger
        // BWC resolution here (only call usesDefaultDistribution on a single javaRestTest task).

        rootProject.pluginManager.apply(JavaPlugin)
        // InternalJavaRestTestPlugin applies RestTestBasePlugin and registers the javaRestTest task
        // (along with javaRestTestClasses, javaRestTestJar, etc.). We use it instead of applying
        // RestTestBasePlugin directly because the task under test is the javaRestTest task itself.
        applyPluginUnderTest(rootProject)
    }

    def "javaRestTest feature-metadata input is named and normalized for cache relocatability"() {
        given:
        // Opt the javaRestTest into usesDefaultDistribution, which is what triggers the input wiring.
        rootProject.tasks.named("javaRestTest").configure { task ->
            task.usesDefaultDistribution("test requires default distribution")
        }

        when:
        def task = rootProject.tasks.getByName("javaRestTest")
        def inputs = inputFileProperties(task)

        then:
        // Classpath normalization: the metadata file's name is stable, and classpath normalization
        // ignores manifest timestamps / line endings, so two checkouts (or two feature-set-only
        // changes) with the same logical file fingerprint will share a cache key.
        inputs["defaultDistroFeatureMetadata"]?.normalizer == "RUNTIME_CLASSPATH"
        inputs["defaultDistroFeatureMetadata"]?.optional == false

        and:
        // The input must not be registered under the legacy anonymous binding that used
        // PathSensitivity.NONE — a regression there would re-pin the cache key to absolute paths.
        !inputs.containsKey(null)
    }

    /**
     * Collects the registered input file properties of a task as {@code name -> [normalizer, optional]}.
     * The normalizer is reported by its {@link org.gradle.internal.execution.model.InputNormalizer} enum
     * constant name (e.g. {@code RUNTIME_CLASSPATH}).
     */
    private static Map<String, Map> inputFileProperties(def task) {
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