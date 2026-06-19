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

class ElasticsearchForeignAccessFuncTest extends AbstractJavaGradleFuncTest {

    def setup() {
        internalBuild()

        buildFile << """
            import org.elasticsearch.gradle.internal.ElasticsearchJavaBasePlugin

            apply plugin: 'java'
            ElasticsearchJavaBasePlugin.enableForeignAccess(project)
        """.stripIndent()
    }

    def "extractForeignApiJar task is registered and produces output"() {
        given:
        clazz('org.acme.Dummy')

        when:
        def result = gradleRunner('extractForeignApiJar', '-g', gradleUserHome).build()

        then:
        result.task(":extractForeignApiJar").outcome in [TaskOutcome.SUCCESS, TaskOutcome.SKIPPED]
    }

    def "compileJava succeeds with enableForeignAccess"() {
        given:
        clazz('org.acme.Dummy')

        when:
        def result = gradleRunner('compileJava', '-g', gradleUserHome).build()

        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS
    }

    def "compileJava is up-to-date on second run (CC reuse)"() {
        given:
        clazz('org.acme.Dummy')

        when:
        gradleRunner('compileJava', '-g', gradleUserHome).build()
        def result = gradleRunner('compileJava', '-g', gradleUserHome).build()

        then:
        result.task(":compileJava").outcome == TaskOutcome.UP_TO_DATE
    }
}
