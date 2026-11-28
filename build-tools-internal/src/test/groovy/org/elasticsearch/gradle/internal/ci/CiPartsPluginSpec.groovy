/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci

import org.elasticsearch.gradle.fixtures.AbstractProjectBuilderPluginSpec
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.testing.Test
import spock.lang.TempDir

class CiPartsPluginSpec extends AbstractProjectBuilderPluginSpec {

    @TempDir
    File workspace

    Project rootProject
    Project project

    @Override
    Class<CiPartsPlugin> getPluginClassUnderTest() {
        return CiPartsPlugin
    }

    def setup() {
        new File(workspace, ".ci/test-parts").mkdirs()
        new File(workspace, CiPartsPlugin.ASSIGNMENT_FILE).text = new CiPartAssignment(
            3,
            [(":lib:yamlRestTest"): new CiPartAssignment.Split(2, CiPartAssignment.SplitBy.YAML_FILE)],
            [(":lib:test"): 1, (":lib:internalClusterTest"): 2, (":lib:yamlRestTest#0"): 2, (":lib:yamlRestTest#1"): 3],
            [(":lib"): 3]
        ).toJson()
        rootProject = buildProject("elasticsearch", null, workspace)
        project = buildProject("lib", rootProject)
        project.pluginManager.apply(JavaPlugin)
        def ict = project.tasks.register("internalClusterTest", Test)
        def yaml = project.tasks.register("yamlRestTest", Test)
        // test tasks behind a lifecycle task are still placed individually
        def aggregate = project.tasks.register("restTests") { dependsOn yaml }
        project.tasks.named("check") { dependsOn ict, aggregate }
        project.tasks.register("precommitThing") { doLast {} }
        project.tasks.named("check") { dependsOn "precommitThing" }
    }

    def "registers one checkPart task per part"() {
        when:
        applyPluginUnderTest(project)

        then:
        (1..3).every { project.tasks.findByName("checkPart$it") != null }
        project.tasks.findByName("checkPart4") == null
    }

    def "each part depends on its share of check"() {
        given:
        applyPluginUnderTest(project)

        expect:
        names(1) == [":lib:test"] as Set
        names(2) == [":lib:internalClusterTest", ":lib:yamlRestTest"] as Set
        // the home part also runs the non-test check work
        names(3) == [":lib:yamlRestTest", ":lib:precommitThing"] as Set
    }

    def "home part runs assemble when requested"() {
        given:
        applyPluginUnderTest(project)
        project.extensions.getByType(CiPartsExtension).includeAssemble.set(true)

        expect:
        names(3).contains(":lib:assemble")
        names(1).contains(":lib:assemble") == false
    }

    def "the union of all parts is exactly what check runs"() {
        given:
        applyPluginUnderTest(project)
        def check = project.tasks.getByName("check")

        expect:
        (names(1) + names(2) + names(3)) == [":lib:test", ":lib:internalClusterTest", ":lib:yamlRestTest", ":lib:precommitThing"] as Set
    }

    private Set<String> names(int part) {
        def task = project.tasks.getByName("checkPart$part")
        return task.taskDependencies.getDependencies(task).collect { it.path } as Set
    }
}
