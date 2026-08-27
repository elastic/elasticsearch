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

import static org.junit.Assume.assumeTrue


class ForeignApiPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = ForeignApiPlugin

    def setup() {
        // Extend the toolchain discovery environment list to include JAVA21_HOME so that a locally
        // installed JDK 21 is found without requiring an auto-provisioning download.
        propertiesFile << "org.gradle.java.installations.fromEnv=" +
            "JAVA_HOME,RUNTIME_JAVA_HOME,JAVA21_HOME,JAVA21_HOME\n"

        buildFile << """
            apply plugin: 'java'
        """.stripIndent()
    }

    // --- ExtractForeignApiTask tests ---

    def "extractForeignApiJar task is registered and produces output"() {
        given:
        clazz('org.acme.Dummy')

        when:
        def result = gradleRunner('assemble').build()

        then:
        result.task(":extractForeignApiJar").outcome == TaskOutcome.SUCCESS
        file("build/jdk21-foreign-api.jar").exists()
    }

    def "extractForeignApiJar is up-to-date on second run"() {
        given:
        clazz('org.acme.Dummy')

        when:
        gradleRunner('assemble').build()
        def result = gradleRunner('assemble').build()

        then:
        result.task(":extractForeignApiJar").outcome == TaskOutcome.UP_TO_DATE
    }

    def "extractForeignApiJar is loaded from build cache after clean"() {
        given:
        clazz('org.acme.Dummy')

        when:
        gradleRunner('assemble', '--build-cache').build()
        gradleRunner('clean').build()
        def result = gradleRunner('extractForeignApiJar', '--build-cache').build()

        then:
        result.task(":extractForeignApiJar").outcome == TaskOutcome.FROM_CACHE
    }

    def "extractForeignApiJar is not registered when minimumRuntimeVersion is not 21"() {
        // The task must be absent from the task graph entirely, not merely skipped.
        given:
        buildFile.text = buildFile.text.replace(
            "plugins.apply(ForeignApiPlugin)",
            """def bp = project.getExtensions().getByType(BuildParameterExtension)
            bp.setMinimumRuntimeVersion(JavaVersion.VERSION_22)
            plugins.apply(ForeignApiPlugin)"""
        )

        when:
        def result = gradleRunner('assemble').build()

        then:
        result.task(":extractForeignApiJar") == null
    }

    // --- ForeignAccessArgumentProvider / --patch-module tests ---

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

    def "compileJava passes --patch-module on JDK 21 minimum runtime"() {
        given:
        clazz('org.acme.Dummy')
        buildFile << """
            tasks.named('compileJava') {
                doLast {
                    def args = it.options.allCompilerArgs
                    def idx = args.indexOf('--patch-module')
                    assert idx >= 0 : "expected --patch-module in compiler args, got: \${args}"
                    assert args[idx + 1].startsWith('java.base=') : "expected java.base= value after --patch-module"
                }
            }
        """.stripIndent()

        when:
        def result = gradleRunner('assemble').build()

        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS
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

    def "forbiddenApisMain rejects direct use of JDK 21 foreign API methods"() {
        given:
        setupForbiddenApiBuild()
        file("src/main/java/org/acme/BadForeignUser.java") << """
            package org.acme;
            import java.lang.foreign.MemorySegment;
            public class BadForeignUser {
                public String bad(MemorySegment s) { return s.getUtf8String(0); }
            }
        """.stripIndent()

        when:
        def result = gradleRunner('forbiddenApisMain').buildAndFail()

        then:
        result.task(":forbiddenApisMain").outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, "Use MemorySegmentAdapter.getString() instead")
    }

    def "forbiddenApisMain rejects direct use of JDK 22+ foreign API methods"() {
        // CheckForbiddenApisTask runs noIsolation() in the Gradle daemon. For JDK 22+ signatures
        // the checker resolves method descriptors from the daemon's own bootclasspath; on JDK 21
        // MemorySegment#getString(long) does not exist there and the checker throws
        // "IO problem while reading files with API signatures". There is no toolchain escape
        // hatch for a noIsolation() worker, so this test requires a JDK 22+ daemon.
        assumeTrue("Requires JDK 22+ daemon for forbidden-apis bootclasspath resolution",
            Runtime.version().feature() >= 22)

        given:
        setupForbiddenApiBuild(false)
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

     // --- CheckForbiddenApisTask tests ---

    /**
     * Builds a project that applies the full foreign-API + forbidden-API stack.
     *
     * @param jdk21Target {@code true} (default) — target JDK 21 (minimumRuntimeVersion=21, uses
     *                    {@code jdk-foreign-signatures}).  {@code false} — target the current
     *                    daemon JDK (always != 21 in the standard build environment) so that the
     *                    JDK 22+ signature file is selected and no stub JAR is produced.
     */
    private void setupForbiddenApiBuild(boolean jdk21Target = true) {
        buildFile.text = ""
        internalBuild()
        def sigFile = jdk21Target ? 'jdk-foreign-signatures' : 'jdk-foreign-signatures22'
        buildFile << """
            import org.elasticsearch.gradle.internal.precommit.ForbiddenApisPrecommitPlugin
            import org.elasticsearch.gradle.internal.precommit.CheckForbiddenApisTask

            apply plugin: 'java'
            apply plugin: ForbiddenApisPrecommitPlugin
        """.stripIndent()
        if (jdk21Target == false) {
            // Drive the plugin into JDK 22+ mode without needing the daemon itself to be JDK 22+.
            buildFile << """
                def bp = project.getExtensions().getByType(org.elasticsearch.gradle.internal.info.BuildParameterExtension)
                bp.setMinimumRuntimeVersion(JavaVersion.toVersion("25"))
            """.stripIndent()
        }
        buildFile << """
            apply plugin: 'elasticsearch.foreign-api'

            tasks.withType(CheckForbiddenApisTask).configureEach {
                replaceSignatureFiles '${sigFile}'
            }
        """.stripIndent()
    }

}
