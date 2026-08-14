/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome


class ForeignApiPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = ForeignApiPlugin

    def setup() {
        // The foreign-api tests compile java.lang.foreign code, which is only non-preview from JDK 22 on.
        // Raise this project's runtime/compiler baseline to the real Elasticsearch baseline so the nested
        // build compiles with --release 25 (foreign is standard) instead of the shared fixture default.
        versionPropertiesFile.text = versionPropertiesFile.text
            .replace('minimumRuntimeJava = 21', 'minimumRuntimeJava = 25')
            .replace('minimumCompilerJava = 21', 'minimumCompilerJava = 25')
        buildFile << """
            apply plugin: 'java'
        """.stripIndent()
    }

    def "compileJava succeeds with foreign-api plugin"() {
        given:
        clazz('org.acme.Dummy')

        when:
        def result = gradleRunner('assemble').build()

        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS
    }

    def "compileJava compiles code that uses MemorySegment without warnings"() {
        given:
        file("src/main/java/org/acme/ForeignUser.java") << """
            package org.acme;
            import java.lang.foreign.MemorySegment;
            public class ForeignUser {
                public long size(MemorySegment s) { return s.byteSize(); }
            }
        """.stripIndent()

        when:
        def result = gradleRunner('assemble').build()

        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS
        result.output.contains("warning") == false
        result.output.contains("error") == false
    }

    def "compileJava is up-to-date on second run"() {
        given:
        clazz('org.acme.Dummy')

        when:
        gradleRunner('assemble').build()
        def result = gradleRunner('assemble').build()

        then:
        result.task(":compileJava").outcome == TaskOutcome.UP_TO_DATE
    }

    def "forbiddenApisMain rejects direct use of foreign API methods that have an adapter"() {
        given:
        setupForbiddenApiBuild()
        file("src/main/java/org/acme/BadForeignUser.java") << """
            package org.acme;
            import java.lang.foreign.MemorySegment;
            public class BadForeignUser {
                public String bad(MemorySegment s) { return s.getString(0); }
            }
        """.stripIndent()

        when:
        def result = gradleRunner('forbiddenApisMain').buildAndFail()

        then:
        result.task(":forbiddenApisMain").outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, "Use MemorySegmentAdapter.getString() instead")
    }

    // --- CheckForbiddenApisTask setup ---

    /**
     * Builds a project that applies the full foreign-API + forbidden-API stack. The Foreign Function
     * & Memory API is standard since JDK 22 (the baseline is JDK 25), so the {@code jdk-foreign-signatures22}
     * file is always used and the checker resolves method descriptors from the daemon's bootclasspath.
     */
    private void setupForbiddenApiBuild() {
        buildFile.text = ""
        internalBuild()
        buildFile << """
            import org.elasticsearch.gradle.internal.precommit.ForbiddenApisPrecommitPlugin
            import org.elasticsearch.gradle.internal.precommit.CheckForbiddenApisTask

            apply plugin: 'java'
            apply plugin: ForbiddenApisPrecommitPlugin
            apply plugin: 'elasticsearch.foreign-api'

            tasks.withType(CheckForbiddenApisTask).configureEach {
                replaceSignatureFiles 'jdk-foreign-signatures22'
            }
        """.stripIndent()
    }

}
