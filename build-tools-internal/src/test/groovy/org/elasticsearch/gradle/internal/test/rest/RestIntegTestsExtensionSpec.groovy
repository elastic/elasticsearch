/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest

import org.gradle.api.Project
import org.gradle.api.tasks.testing.Test
import org.gradle.testfixtures.ProjectBuilder
import spock.lang.Specification

/**
 * Unit tests for {@link RestIntegTestsExtension}.
 * <p>
 * Uses a real {@link ProjectBuilder}-backed project to exercise the name-based REST integ test
 * detection path (plain {@link Test} tasks registered via {@link RestIntegTestsExtension#register}).
 * The {@code StandaloneRestIntegTestTask} type-based path is covered by legacy functional tests
 * (e.g. {@code LegacyYamlRestTestPluginFuncTest}).
 */
class RestIntegTestsExtensionSpec extends Specification {

    Project project = ProjectBuilder.builder().build()
    RestIntegTestsExtension extension = new RestIntegTestsExtension(project)

    def "register returns a TaskProvider for a plain Test task"() {
        when:
        def provider = extension.register("myRestTest") {}

        then:
        provider != null
        provider.name == "myRestTest"
        project.tasks.findByName("myRestTest") != null
        project.tasks.findByName("myRestTest") instanceof Test
    }

    def "configureEach applies the action to tasks registered via register()"() {
        given:
        List<String> configured = []

        when:
        extension.configureEach { task -> configured << task.name }
        extension.register("restTest1") {}
        extension.register("restTest2") {}
        // trigger task realisation
        project.tasks.getByName("restTest1")
        project.tasks.getByName("restTest2")

        then:
        configured.containsAll(["restTest1", "restTest2"])
    }

    def "configureEach does NOT apply to plain Test tasks not registered via restTests.register()"() {
        given:
        List<String> configured = []

        when:
        extension.configureEach { task -> configured << task.name }
        project.tasks.register("plainTest", Test.class) {}
        extension.register("enrolledRestTest") {}
        project.tasks.getByName("plainTest")
        project.tasks.getByName("enrolledRestTest")

        then:
        configured == ["enrolledRestTest"]
        !configured.contains("plainTest")
    }

    def "getTasks returns only enrolled REST integ test tasks"() {
        given:
        extension.register("yamlRestTest") {}
        extension.register("javaRestTest") {}
        project.tasks.register("unitTest", Test.class) {}

        when:
        def restTasks = extension.getTasks().collect { it.name }

        then:
        restTasks.containsAll(["yamlRestTest", "javaRestTest"])
        !restTasks.contains("unitTest")
    }

    def "action registered via configureEach before register() still fires for that task"() {
        given:
        List<String> order = []

        when:
        // configureEach registered BEFORE the task exists
        extension.configureEach { task -> order << ("configureEach:" + task.name) }
        extension.register("lateRestTest") { order << ("register:" + it.name) }
        project.tasks.getByName("lateRestTest")

        then:
        // configureEach fires before the register action (Gradle execution order guarantee)
        order.indexOf("configureEach:lateRestTest") < order.indexOf("register:lateRestTest")
    }
}
