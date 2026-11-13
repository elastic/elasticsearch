/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

/**
 * Runs {@code mergeMuted} against a real git repository: a branch that removed one mute and appended another is merged into a
 * branch that appended a different mute, which conflicts at the end of the file.
 */
class MergeMutedTestsTaskFuncTest extends AbstractGradleFuncTest {

    static final String A = """\
- class: org.acme.ATests
  method: testA
  issue: https://github.com/elastic/elasticsearch/issues/1
"""
    static final String B = """\
- class: org.acme.BTests
  method: testB {p0=true}
  issue: https://github.com/elastic/elasticsearch/issues/2
"""
    static final String X = """\
- class: org.acme.XTests
  issue: https://github.com/elastic/elasticsearch/issues/3
"""
    static final String C = """\
- class: org.acme.CTests
  tasks:
  - :server:test
  method: testC
  issue: https://github.com/elastic/elasticsearch/issues/4
"""
    static final String D = """\
- class: org.acme.DTests
  method: testD
  issue: https://github.com/elastic/elasticsearch/issues/5
"""

    def setup() {
        buildFile << """
            import org.elasticsearch.gradle.internal.test.MergeMutedTestsTask

            plugins {
              // bring build-tools-internal onto the script classpath
              id 'elasticsearch.global-build-info'
            }

            tasks.register('mergeMuted', MergeMutedTestsTask)
        """
        file("muted-tests.yml").text = "tests:\n" + A + B + X
        execute("git init")
        execute('git config user.email "build-tool@elastic.co"')
        execute('git config user.name "Build tool"')
        execute('git config commit.gpgsign false')
        file(".gitignore") << """
        .gradle/
        build/
        """.stripIndent()
        execute("git add .")
        execute('git commit -m "Initial"')
        if (execute("git branch --show-current").strip() != "main") {
            // some CI doesn't have main set as the default branch
            execute("git checkout -b main")
        }
    }

    def "conflicting merge is resolved and staged"() {
        given:
        execute("git checkout -b incoming")
        file("muted-tests.yml").text = "tests:\n" + A + X + C // removes B, appends C
        execute("git commit -am incoming")
        execute("git checkout main")
        file("muted-tests.yml").text = "tests:\n" + A + B + X + D // appends D
        execute("git commit -am receiving")
        execute("git merge incoming", testProjectDir.root, true)
        assert execute("git ls-files -u").contains("muted-tests.yml")

        when:
        def result = gradleRunner("mergeMuted").build()

        then:
        result.task(":mergeMuted").outcome == TaskOutcome.SUCCESS
        result.output.contains("applied 1 addition(s) and 1 removal(s)")
        file("muted-tests.yml").text == "tests:\n" + A + X + D + C
        execute("git ls-files -u").strip().isEmpty()
        execute("git diff --cached --name-only").strip() == "muted-tests.yml"
    }

    def "no conflict is a no-op"() {
        when:
        def result = gradleRunner("mergeMuted").build()

        then:
        result.task(":mergeMuted").outcome == TaskOutcome.SUCCESS
        result.output.contains("No merge conflict in muted-tests.yml")
        file("muted-tests.yml").text == "tests:\n" + A + B + X
    }
}
