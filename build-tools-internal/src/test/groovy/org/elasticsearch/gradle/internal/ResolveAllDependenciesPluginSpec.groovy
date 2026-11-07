/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractProjectBuilderPluginSpec
import org.gradle.api.Project

class ResolveAllDependenciesPluginSpec extends AbstractProjectBuilderPluginSpec {

    @Override
    Class<ResolveAllDependenciesPlugin> getPluginClassUnderTest() {
        return ResolveAllDependenciesPlugin
    }

    def "registers the resolveAllDependencies task"() {
        given:
        Project project = buildProject("consumer")

        when:
        applyPluginUnderTest(project)
        def task = project.tasks.getByName("resolveAllDependencies")

        then:
        task instanceof ResolveAllDependencies
    }

    def "sets resolveJavaToolChain on the root project"() {
        given:
        Project rootProject = buildProject("root")

        when:
        applyPluginUnderTest(rootProject)
        ResolveAllDependencies task = rootProject.tasks.getByName("resolveAllDependencies") as ResolveAllDependencies

        then:
        task.resolveJavaToolChain.get()
    }

    def "disables resolveAllDependencies for docker distribution projects"() {
        given:
        Project rootProject = buildProject("root")
        Project distributionProject = buildProject("distribution", rootProject)
        Project dockerProject = buildProject("docker", distributionProject)

        when:
        applyPluginUnderTest(dockerProject)
        ResolveAllDependencies task = dockerProject.tasks.getByName("resolveAllDependencies") as ResolveAllDependencies

        then:
        task.enabled == false
    }

    def "makes libs cli resolveAllDependencies depend on spotlessJavaCheck"() {
        given:
        Project rootProject = buildProject("root")
        Project libsProject = buildProject("libs", rootProject)
        Project cliProject = buildProject("cli", libsProject)
        cliProject.tasks.register("spotlessJavaCheck")

        when:
        applyPluginUnderTest(cliProject)
        def task = cliProject.tasks.getByName("resolveAllDependencies")
        def dependencies = task.taskDependencies.getDependencies(task)*.name

        then:
        "spotlessJavaCheck" in dependencies
    }
}
