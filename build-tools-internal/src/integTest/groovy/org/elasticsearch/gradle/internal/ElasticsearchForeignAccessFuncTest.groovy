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
            import org.elasticsearch.gradle.internal.ElasticsearchJavaBasePlugin

            apply plugin: 'java'
            ElasticsearchJavaBasePlugin.enableForeignAccess(project)
        """.stripIndent()
    }

    private static boolean isJdk21() {
        return Runtime.version().feature() == 21
    }

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

    def "compileJava succeeds with enableForeignAccess"() {
        given:
        clazz('org.acme.Dummy')

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
}
