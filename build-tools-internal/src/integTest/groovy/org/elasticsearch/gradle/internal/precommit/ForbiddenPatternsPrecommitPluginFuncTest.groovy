/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.testkit.runner.TaskOutcome

class ForbiddenPatternsPrecommitPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<ForbiddenPatternsPrecommitPlugin> pluginClassUnderTest = ForbiddenPatternsPrecommitPlugin.class

    def "detects tab violations and reports problems"() {
        given:
        buildFile << """
        apply plugin:'java'
        """
        file("src/main/java/Foo.java").text = "\tpublic class Foo {}"

        when:
        def result = gradleRunner("precommit").buildAndFail()

        then:
        result.task(":forbiddenPatterns").outcome == TaskOutcome.FAILED
        assertOutputContains(result.getOutput(), "invalid pattern")
        // Verify the problems report is generated
        assertOutputContains(result.getOutput(), "Problems report is available at")
    }

    def "detects nocommit violations and reports problems"() {
        given:
        buildFile << """
        apply plugin:'java'
        """
        file("src/main/java/Bar.java").text = "public class Bar {} // nocommit"

        when:
        def result = gradleRunner("precommit").buildAndFail()

        then:
        result.task(":forbiddenPatterns").outcome == TaskOutcome.FAILED
        assertOutputContains(result.getOutput(), "invalid pattern")
    }

    def "collects multiple violations across files"() {
        given:
        buildFile << """
        apply plugin:'java'
        """
        file("src/main/java/File1.java").text = "\tpublic class File1 {}"
        file("src/main/java/File2.java").text = "\tpublic class File2 {}"

        when:
        def result = gradleRunner("precommit").buildAndFail()

        then:
        result.task(":forbiddenPatterns").outcome == TaskOutcome.FAILED
        // Should report multiple violations (not fail on first)
        assertOutputContains(result.getOutput(), "invalid pattern")
    }

    def "succeeds when no violations"() {
        given:
        buildFile << """
        apply plugin:'java'
        """
        file("src/main/java/Clean.java").text = "public class Clean {}"

        when:
        def result = gradleRunner("precommit").build()

        then:
        result.task(":forbiddenPatterns").outcome == TaskOutcome.SUCCESS
    }
}
