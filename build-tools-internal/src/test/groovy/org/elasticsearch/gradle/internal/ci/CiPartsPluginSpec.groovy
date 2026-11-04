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
        new File(workspace, ".ci/test-parts/bc-upgrade").mkdirs()
        new File(workspace, CiPartsPlugin.BC_UPGRADE_ASSIGNMENT_FILE).text = new CiPartAssignment(
            2,
            [(":lib:bcUpgradeTest"): new CiPartAssignment.Split(3, CiPartAssignment.SplitBy.CLASS)],
            [(":lib:bcUpgradeTest#0"): 1, (":lib:bcUpgradeTest#1"): 2, (":lib:bcUpgradeTest#2"): 2],
            [(":lib"): 2]
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

    def "the other tasks of a requested part should run after its run-first tasks, longest first"() {
        given:
        new File(workspace, CiPartsPlugin.ASSIGNMENT_FILE).text = new CiPartAssignment(
            3,
            [(":lib:yamlRestTest"): new CiPartAssignment.Split(2, CiPartAssignment.SplitBy.YAML_FILE)],
            [(":lib:test"): 1, (":lib:internalClusterTest"): 2, (":lib:yamlRestTest#0"): 2, (":lib:yamlRestTest#1"): 3],
            [(":lib"): 3],
            // :gone:test no longer exists and is skipped
            [":lib:yamlRestTest", ":gone:test", ":lib:internalClusterTest"]
        ).toJson()
        project.gradle.startParameter.taskNames = ["checkPart2", "checkPart3"]
        // check can reach tasks of other projects (or, for the root project, of included builds that already ran)
        def other = buildProject("other", rootProject)
        def otherThing = other.tasks.register("otherThing") { doLast {} }
        project.tasks.named("check") { dependsOn otherThing }
        applyPluginUnderTest(project)
        // building the graph resolves the part tasks' dependencies, which adds the ordering
        names(2)
        names(3)
        names(1)

        expect:
        // only the ordering this plugin adds; the java plugin has test tasks run after jar
        added("yamlRestTest") == [] as Set
        added("internalClusterTest") == [":lib:yamlRestTest"] as Set
        added("precommitThing") == [":lib:yamlRestTest"] as Set
        // part 1 was not requested, so its tasks are not reordered
        added("test") == [] as Set
        // the other project's task is left to that project
        names(3).contains(":other:otherThing")
        otherThing.get().shouldRunAfter.getDependencies(otherThing.get()).isEmpty()
    }

    def "parts are not reordered unless part tasks are requested"() {
        given:
        new File(workspace, CiPartsPlugin.ASSIGNMENT_FILE).text = new CiPartAssignment(3, [:], [(":lib:test"): 1], [(":lib"): 1], [":lib:test"]).toJson()
        project.gradle.startParameter.taskNames = ["check"]
        applyPluginUnderTest(project)
        names(1)

        expect:
        added("precommitThing") == [] as Set
    }

    def "requested parts are read from checkPart task requests"() {
        expect:
        CiPartsPlugin.requestedParts(CiPartsPlugin.CHECK, ["checkPart3", ":server:checkPart12", "check", "checkPart", "checkPart2x", "bcUpgradeTestPart1"]) == [3, 12] as Set
        CiPartsPlugin.requestedParts(CiPartsPlugin.BC_UPGRADE, ["checkPart3", "bcUpgradeTestPart1", ":qa:rolling-upgrade:bcUpgradeTestPart2"]) == [1, 2] as Set
    }

    def "bcUpgradeTest parts split the bcUpgradeTest task by class"() {
        given:
        project.gradle.startParameter.taskNames = ["bcUpgradeTestPart2"]
        project.tasks.register("bcUpgradeTest", Test)
        applyPluginUnderTest(project)

        expect:
        bcNames(1) == [":lib:bcUpgradeTest"] as Set
        bcNames(2) == [":lib:bcUpgradeTest"] as Set
        // only bcUpgradeTest is in these parts, not check's work
        project.tasks.getByName("bcUpgradeTest").inputs.properties["ciShards"] == "CLASS=3:1,2"
        // the check suite's split task is untouched by bcUpgradeTestPartN requests
        project.tasks.getByName("yamlRestTest").systemProperties["tests.rest.shard"] == null
    }

    def "projects without bcUpgradeTest get empty bcUpgradeTest parts but for assemble"() {
        given:
        applyPluginUnderTest(project)
        project.extensions.getByType(CiPartsExtension).includeAssemble.set(true)

        expect:
        bcNames(1) == [] as Set
        // :lib's home part in the bcUpgradeTest assignment
        bcNames(2) == [":lib:assemble"] as Set
    }

    def "a bcUpgradeTest task missing from the assignment runs whole in exactly one part"() {
        given:
        def other = buildProject("other", rootProject)
        other.tasks.register("bcUpgradeTest", Test)
        other.gradle.startParameter.taskNames = ["bcUpgradeTestPart1"]
        applyPluginUnderTest(other)
        def deps = { int part ->
            def task = other.tasks.getByName("bcUpgradeTestPart$part")
            task.taskDependencies.getDependencies(task).collect { it.path } as Set
        }

        expect:
        [deps(1), deps(2)].count { it == [":other:bcUpgradeTest"] as Set } == 1
        [deps(1), deps(2)].count { it.isEmpty() } == 1
        other.tasks.getByName("bcUpgradeTest").inputs.properties["ciShards"] == null
    }

    def "split yaml tasks only run the shards of the requested parts"() {
        given:
        project.gradle.startParameter.taskNames = ["checkPart2"]
        applyPluginUnderTest(project)

        expect:
        project.tasks.getByName("yamlRestTest").systemProperties["tests.rest.shard"] == "2:0"
        project.tasks.getByName("yamlRestTest").inputs.properties["ciShards"] == "YAML_FILE=2:0"
        // unsplit tasks are untouched
        project.tasks.getByName("internalClusterTest").systemProperties["tests.rest.shard"] == null
    }

    def "split tasks run whole when every shard or no shard is requested"() {
        given:
        project.gradle.startParameter.taskNames = taskNames
        applyPluginUnderTest(project)

        expect:
        project.tasks.getByName("yamlRestTest").systemProperties["tests.rest.shard"] == null

        where:
        taskNames << [["checkPart2", "checkPart3"], ["checkPart1"], ["check"]]
    }

    def "class shards keep each class and its nested classes in exactly one shard"() {
        given:
        def classesDir = new File(workspace, "classes")
        def names = (0..<300).collect { "org/foo/Foo${it}IT" }
        names.each {
            new File(classesDir, "${it}.class").with { parentFile.mkdirs(); text = "" }
            new File(classesDir, "${it}\$Inner.class").text = ""
        }
        new File(classesDir, "org/foo/resource.txt").text = ""

        when:
        def kept = (0..<3).collect { shard ->
            project.fileTree(classesDir).matching { exclude(new ClassShardSpec(3, [shard] as Set)) }.files
                .collect { classesDir.toPath().relativize(it.toPath()).toString() } as Set
        }

        then:
        // every class file is kept by exactly one shard; resources are kept by all of them
        names.every { n -> kept.count { it.contains("${n}.class".toString()) } == 1 }
        names.every { n -> kept.findIndexOf { it.contains("${n}.class".toString()) } == kept.findIndexOf { it.contains("${n}\$Inner.class".toString()) } }
        kept.every { it.contains("org/foo/resource.txt") }
        kept.every { it.size() > 50 }
    }

    /** The tasks {@code taskName} should run after, other than its project's jar task. */
    private Set<String> added(String taskName) {
        def task = project.tasks.getByName(taskName)
        return (task.shouldRunAfter.getDependencies(task).collect { it.path } as Set) - ":lib:jar"
    }

    private Set<String> bcNames(int part) {
        def task = project.tasks.getByName("bcUpgradeTestPart$part")
        return task.taskDependencies.getDependencies(task).collect { it.path } as Set
    }

    private Set<String> names(int part) {
        def task = project.tasks.getByName("checkPart$part")
        return task.taskDependencies.getDependencies(task).collect { it.path } as Set
    }
}
