/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

import groovy.json.JsonSlurper

/**
 * Functional test for the <b>per-project</b> half of flakiness resolution, i.e. the contract
 * {@link FlakinessProjectResolvePlugin} alone is responsible for. The full
 * {@code resolve -> compile -> scan} flow, including the bytecode enrichment and the emitted batch commands,
 * is covered separately by {@code FlakinessResolvePluginFuncTest}.
 *
 * <p>Two properties are checked here, because both are the plugin's own and neither is observable from the
 * scan step:
 * <ol>
 *   <li><b>Inertness.</b> The plugin is applied to every test project in the real build, so it must register
 *       nothing at all unless {@code -Pflakiness.resolve} asked for it.</li>
 *   <li><b>Self-selection.</b> The task is invoked <em>unqualified</em> and each project decides on its own
 *       whether a ref lands in one of its source sets, so the project that owns nothing resolves nothing. It
 *       must nevertheless report a full model, its class dirs and its source-set dispositions: the scan step
 *       may have to run a subclass compiled there, which needs that project's own {@code Test} tasks.</li>
 * </ol>
 */
class FlakinessProjectResolvePluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    @Override
    Class<? extends Plugin> getPluginClassUnderTest() {
        FlakinessProjectResolvePlugin.class
    }

    def setup() {
        // Exactly how ElasticsearchTestBasePlugin brings the plugin in: applied unconditionally, gated inside.
        def applyPlugin = """
            plugins { id 'java' }
            pluginManager.apply(org.elasticsearch.gradle.internal.flakiness.FlakinessProjectResolvePlugin)
        """

        subProject(":owner") << applyPlugin
        subProject(":bystander") << applyPlugin

        javaTestClass("owner", "com/owner/OwnedTests", "class OwnedTests {}")
        // The bystander has test sources too, so self-selecting out is a real decision about ref ownership
        // rather than the trivial "this project has no tests" case.
        javaTestClass("bystander", "com/bystander/BystanderTests", "class BystanderTests {}")

        file("flakiness-refs.json").text = """
            { "mergeBase": "test",
              "refs": [ { "source": "unmute", "className": "com.owner.OwnedTests" } ] }
        """
    }

    def "the plugin registers nothing unless -Pflakiness.resolve is set"() {
        when: "the task is requested without the gate property"
        def result = gradleRunner(FlakinessProjectResolvePlugin.TASK_NAME).buildAndFail()

        then: "no project registered it, so there is nothing to run"
        result.output.contains("Task '${FlakinessProjectResolvePlugin.TASK_NAME}' not found")
    }

    def "each project self-selects and writes only its own share of the answer"() {
        when: "the task is invoked UNQUALIFIED, so it runs in every project that applied the plugin"
        def result = gradleRunner("-Pflakiness.resolve", FlakinessProjectResolvePlugin.TASK_NAME).build()

        then: "it ran in both projects, including the one that owns nothing"
        [":owner", ":bystander"].every {
            result.task("${it}:${FlakinessProjectResolvePlugin.TASK_NAME}").outcome == TaskOutcome.SUCCESS
        }

        and: "the owner resolved the ref against its own authoritative model"
        def resolved = projectTargets("owner").resolved
        resolved.size() == 1
        resolved[0].refIndex == 0
        def target = resolved[0].target
        target.gradleProject == ":owner"
        target.fqcn == "com.owner.OwnedTests"
        target.sourceSet == "test"
        target.kind == "test"
        target.runnableTasks == [":owner:test"]
        target.skipReason == null

        and: "the owner reported its class dirs, spanning main as well as its test source set"
        def ownerDirs = classDirsOf("owner").collect { it.replace('\\', '/') }
        ownerDirs.any { it.endsWith("owner/build/classes/java/test") }
        ownerDirs.any { it.endsWith("owner/build/classes/java/main") }

        and: "the bystander resolved NOTHING but still reported a full model, class dirs and dispositions"
        projectTargets("bystander").resolved.isEmpty()
        def bystanderModel = projectModel("bystander")
        // Owning no ref does not make a project irrelevant: the scan may have to run a subclass compiled here,
        // which needs this project's own Test tasks. So the model is captured in full either way.
        !bystanderModel.sourceSets.isEmpty()
        bystanderModel.testTasks.find { it.name == "test" }.enabled == true
        def bystanderDirs = classDirsOf("bystander").collect { it.replace('\\', '/') }
        bystanderDirs.any { it.endsWith("bystander/build/classes/java/test") }
        // The disposition is the part the scan joins on, keyed by compiled-output directory.
        def bystanderDisp = projectTargets("bystander").dispositions
        bystanderDisp.size() == 1
        bystanderDisp[0].sourceSet == "test"
        bystanderDisp[0].runnableTasks == [":bystander:test"]

        and: "the configuration cache entry was stored without problems"
        result.output.contains("Configuration cache entry stored")
    }

    private Object projectTargets(String project) {
        new JsonSlurper().parse(file("${FlakinessProjectResolvePlugin.TARGETS_DIR}/${project}.json"))
    }

    private Object projectModel(String project) {
        new JsonSlurper().parse(file("${project}/build/${FlakinessProjectResolvePlugin.MODEL_FILE}"))
    }

    private List<String> classDirsOf(String project) {
        projectTargets(project).classDirs.collect { it as String }
    }

    private void javaTestClass(String project, String internalName, String body) {
        file("${project}/src/test/java/${internalName}.java").text = """
            package ${internalName.substring(0, internalName.lastIndexOf('/')).replace('/', '.')};
            ${body}
        """
    }
}
