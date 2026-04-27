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

    def "detects tab violations and reports structured problems"() {
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
        assertOutputContains(result.getOutput(), "Problems report is available at")

        and: "problems report contains forbidden-patterns group with correct problem"
        assertProblemsReportContains("forbidden-patterns")
        assertProblemsReportContains("elasticsearch-build")
        assertProblemsReportContains("precommit")
    }

    def "detects nocommit violations and reports structured problems"() {
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

        and: "problems report contains nocommit problem"
        assertProblemsReportContains("forbidden-patterns")
        def diagnostics = problemsReportDiagnostics()
        diagnostics.any { it.contextualLabel?.contains("nocommit") }
    }

    def "collects multiple violations across files into problems report"() {
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

        and: "problems report has at least 2 diagnostics (one per file)"
        assertProblemsReportHasAtLeast(2)
        assertProblemsReportSeverity("tab", "ERROR")
    }

    def "multiple rule violations in same file reported as separate problems"() {
        given:
        buildFile << """
        apply plugin:'java'
        """
        // File with both tab and nocommit violations
        file("src/main/java/Bad.java").text = "\tpublic class Bad {} // nocommit"

        when:
        def result = gradleRunner("precommit").buildAndFail()

        then:
        result.task(":forbiddenPatterns").outcome == TaskOutcome.FAILED

        and: "problems report has at least 2 diagnostics (one per rule)"
        assertProblemsReportHasAtLeast(2)
        def diagnostics = problemsReportDiagnostics()
        // Should have both tab and nocommit problems
        diagnostics.any { it.contextualLabel?.toLowerCase()?.contains("nocommit") }
    }

    def "problems have solutions in report"() {
        given:
        buildFile << """
        apply plugin:'java'
        """
        file("src/main/java/Foo.java").text = "\tpublic class Foo {}"

        when:
        def result = gradleRunner("precommit").buildAndFail()

        then:
        result.task(":forbiddenPatterns").outcome == TaskOutcome.FAILED

        and: "each reported problem has a solution"
        def diagnostics = problemsReportDiagnostics()
        diagnostics.every { it.solutions != null && !it.solutions.isEmpty() }
    }

    def "succeeds when no violations and problems report is empty"() {
        given:
        buildFile << """
        apply plugin:'java'
        """
        file("src/main/java/Clean.java").text = "public class Clean {}"

        when:
        def result = gradleRunner("precommit").build()

        then:
        result.task(":forbiddenPatterns").outcome == TaskOutcome.SUCCESS

        and: "the problems report contains no diagnostics at all"
        def diagnostics = problemsReportDiagnostics()
        diagnostics.isEmpty()
    }
}
