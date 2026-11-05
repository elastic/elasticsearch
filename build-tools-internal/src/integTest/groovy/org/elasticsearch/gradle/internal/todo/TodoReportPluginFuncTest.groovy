/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.testkit.runner.TaskOutcome

/**
 * Runs {@code todoReport} against a real git checkout so blame, module path, and the JSON report are covered together.
 */
class TodoReportPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<TodoReportPlugin> pluginClassUnderTest = TodoReportPlugin

    def "attributes todo and fixme comments with git blame"() {
        given:
        def server = subProject(":server")
        server << """
            apply plugin: 'java'
            plugins.apply(org.elasticsearch.gradle.internal.todo.TodoReportPlugin)
        """
        def source = file("server/src/main/java/org/example/Foo.java")
        source.text = """package org.example;
public class Foo {
    // TODO(ada): ship the report
}
"""
        git("init")
        git("config", "user.email", "ada@example.com")
        git("config", "user.name", "Ada Lovelace")
        git("config", "commit.gpgsign", "false")
        git("config", "core.autocrlf", "false")
        git("add", "server/src/main/java/org/example/Foo.java")
        git("commit", "-m", "add todo")
        source.text = """package org.example;
public class Foo {
    // TODO(ada): ship the report
    void bar() {
        // FIXME: handle the edge
    }
}
"""
        git("add", "server/src/main/java/org/example/Foo.java")
        git("commit", "-m", "add fixme")

        when:
        def result = gradleRunner(":server:todoReport").build()

        then:
        result.task(":server:todoReport").outcome == TaskOutcome.SUCCESS
        def report = new groovy.json.JsonSlurper().parse(file("server/build/reports/todo-report/todos.json"))
        report.items.size() == 2

        def todo = report.items[0]
        todo.marker == "TODO"
        todo.message == "ship the report"
        todo.assignee == "ada"
        todo.file == "server/src/main/java/org/example/Foo.java"
        todo.module == ":server"
        todo.area == "server"
        todo.sourceSet == "main"
        todo.line == 3
        todo.authorName == "Ada Lovelace"
        todo.authorEmail == "ada@example.com"
        todo.blameStatus == "ok"
        todo.commit ==~ /[0-9a-f]{40}/
        todo.authoredDate ==~ /\d{4}-\d{2}-\d{2}/
        todo.authoredAt.startsWith(todo.authoredDate)

        def fixme = report.items[1]
        fixme.marker == "FIXME"
        fixme.message == "handle the edge"
        fixme.assignee == null
        fixme.line == 5
        fixme.blameStatus == "ok"
        fixme.commit ==~ /[0-9a-f]{40}/
        fixme.commit != todo.commit
    }

    def "omits untracked files unless include-untracked is set"() {
        given:
        def server = subProject(":server")
        server << """
            apply plugin: 'java'
            plugins.apply(org.elasticsearch.gradle.internal.todo.TodoReportPlugin)
        """
        def tracked = file("server/src/main/java/org/example/Foo.java")
        tracked.text = """package org.example;
public class Foo {
    // TODO: tracked
}
"""
        def untracked = file("server/src/main/java/org/example/Bar.java")
        untracked.text = """package org.example;
public class Bar {
    // TODO: not tracked
}
"""
        git("init")
        git("config", "user.email", "ada@example.com")
        git("config", "user.name", "Ada Lovelace")
        git("config", "commit.gpgsign", "false")
        git("config", "core.autocrlf", "false")
        git("add", "server/src/main/java/org/example/Foo.java")
        git("commit", "-m", "add tracked")

        when:
        def skipped = gradleRunner(":server:todoReport").build()

        then:
        skipped.task(":server:todoReport").outcome == TaskOutcome.SUCCESS
        def without = new groovy.json.JsonSlurper().parse(file("server/build/reports/todo-report/todos.json"))
        without.items.collect { it.message } == ["tracked"]

        when:
        def included = gradleRunner(":server:todoReport", "--include-untracked").build()

        then:
        included.task(":server:todoReport").outcome == TaskOutcome.SUCCESS
        def withUntracked = new groovy.json.JsonSlurper().parse(file("server/build/reports/todo-report/todos.json"))
        withUntracked.items.collect { it.message }.sort() == ["not tracked", "tracked"]
        withUntracked.items.find { it.message == "not tracked" }.blameStatus == "untracked"
    }

    private void git(String... args) {
        def process = new ProcessBuilder(["git", *args]).directory(projectDir).redirectErrorStream(true).start()
        def text = process.inputStream.getText("UTF-8")
        assert process.waitFor() == 0 : text
    }
}
