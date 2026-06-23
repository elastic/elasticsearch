/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractJavaGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

import static org.junit.Assume.assumeFalse
import static org.junit.Assume.assumeTrue

class ElasticsearchForeignAccessFuncTest extends AbstractJavaGradleFuncTest {

    def setup() {
        internalBuild()

        buildFile << """
            apply plugin: 'java'
            apply plugin: 'elasticsearch.foreign-api'
        """.stripIndent()
    }

    private static boolean isJdk21() {
        return Runtime.version().feature() == 21
    }

    // --- ExtractForeignApiTask tests ---

    def "extractForeignApiJar task is registered and produces output"() {
        assumeTrue("Requires JDK 21", isJdk21())

        given:
        clazz('org.acme.Dummy')

        when:
        def result = gradleRunner('extractForeignApiJar', '-g', gradleUserHome).build()

        then:
        result.task(":extractForeignApiJar").outcome == TaskOutcome.SUCCESS
        file("build/jdk21-foreign-api.jar").exists()
    }

    def "extractForeignApiJar is up-to-date on second run"() {
        assumeTrue("Requires JDK 21", isJdk21())

        given:
        clazz('org.acme.Dummy')

        when:
        gradleRunner('extractForeignApiJar', '-g', gradleUserHome).build()
        def result = gradleRunner('extractForeignApiJar', '-g', gradleUserHome).build()

        then:
        result.task(":extractForeignApiJar").outcome == TaskOutcome.UP_TO_DATE
    }

    def "extractForeignApiJar is loaded from build cache after clean"() {
        assumeTrue("Requires JDK 21", isJdk21())

        given:
        clazz('org.acme.Dummy')

        when:
        gradleRunner('extractForeignApiJar', '--build-cache', '-g', gradleUserHome).build()
        gradleRunner('clean', '-g', gradleUserHome).build()
        def result = gradleRunner('extractForeignApiJar', '--build-cache', '-g', gradleUserHome).build()

        then:
        result.task(":extractForeignApiJar").outcome == TaskOutcome.FROM_CACHE
    }

    def "extractForeignApiJar is skipped on non-JDK 21"() {
        assumeFalse("Requires non-JDK 21", isJdk21())

        given:
        clazz('org.acme.Dummy')

        when:
        def result = gradleRunner('extractForeignApiJar', '-g', gradleUserHome).build()

        then:
        result.task(":extractForeignApiJar").outcome == TaskOutcome.SKIPPED
    }

    // --- ForeignAccessArgumentProvider / --patch-module tests ---

    def "compileJava succeeds with enableForeignAccess"() {
        given:
        clazz('org.acme.Dummy')

        when:
        def result = gradleRunner('compileJava', '-g', gradleUserHome).build()

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
        def result = gradleRunner('compileJava', '-g', gradleUserHome).build()

        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS
        result.output.contains("warning") == false
        result.output.contains("error") == false
    }

    def "compileJava passes --patch-module on JDK 21"() {
        assumeTrue("Requires JDK 21", isJdk21())

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
        def result = gradleRunner('compileJava', '-g', gradleUserHome).build()

        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS
    }

    def "compileJava is up-to-date on second run"() {
        given:
        clazz('org.acme.Dummy')

        when:
        gradleRunner('compileJava', '-g', gradleUserHome).build()
        def result = gradleRunner('compileJava', '-g', gradleUserHome).build()

        then:
        result.task(":compileJava").outcome == TaskOutcome.UP_TO_DATE
    }

    // --- CheckForbiddenApisTask tests ---

    private void setupForbiddenApiBuild() {
        buildFile.text = ""
        internalBuild()
        def sigFile = isJdk21() ? 'jdk-foreign-signatures' : 'jdk-foreign-signatures22'
        buildFile << """
            import org.elasticsearch.gradle.internal.precommit.ForbiddenApisPrecommitPlugin
            import org.elasticsearch.gradle.internal.precommit.CheckForbiddenApisTask

            apply plugin: 'java'
            apply plugin: ForbiddenApisPrecommitPlugin
            apply plugin: 'elasticsearch.foreign-api'

            tasks.withType(CheckForbiddenApisTask).configureEach {
                replaceSignatureFiles '${sigFile}'
            }
        """.stripIndent()
    }

    def "forbiddenApisMain rejects direct use of JDK 21 foreign API methods"() {
        assumeTrue("Requires JDK 21 for preview API method names", isJdk21())

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
        def result = gradleRunner('forbiddenApisMain', '-g', gradleUserHome).buildAndFail()

        then:
        result.task(":forbiddenApisMain").outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, "Use MemorySegmentAdapter.getString() instead")
    }

    def "forbiddenApisMain rejects direct use of JDK 22+ foreign API methods"() {
        assumeFalse("Requires JDK 22+", isJdk21())

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
        def result = gradleRunner('forbiddenApisMain', '-g', gradleUserHome).buildAndFail()

        then:
        result.task(":forbiddenApisMain").outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, "Use MemorySegmentAdapter.getString() instead")
    }
}
