/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGitAwareGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class InternalBwcGitPluginFuncTest extends AbstractGitAwareGradleFuncTest {

    def setup() {
        configurationCacheCompatible = false
        internalBuild()
        buildFile << """
            import org.elasticsearch.gradle.Version;
            apply plugin: org.elasticsearch.gradle.internal.InternalBwcGitPlugin

            bwcGitConfig {
                 bwcVersion = project.provider { Version.fromString("7.9.1") }
                 bwcBranch = project.provider { "7.9" }
                 checkoutDir = project.provider{file("build/checkout")}
            }
        """
        execute("git branch origin/7.9", file("cloned"))
    }

    def "current repository can be cloned"() {
        when:
        def result = gradleRunner("createClone", '--stacktrace').build()
        then:
        result.task(":createClone").outcome == TaskOutcome.SUCCESS
        file("cloned/build/checkout/build.gradle").exists()
        file("cloned/build/checkout/settings.gradle").exists()
    }

    def "git checkout is skipped when DRA build ID is configured"() {
        given:
        buildFile << """
            plugins.getPlugin(org.elasticsearch.gradle.internal.InternalBwcGitPlugin)
                .configureDraBuildId(project.providers.provider { "7.9.1-abc12345" })
        """
        when:
        def result = gradleRunner(
            "checkoutBwcBranch",
            '--stacktrace',
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin"
        ).build()
        then: "all git tasks are skipped because the DRA build ID is non-empty"
        result.task(":createClone").outcome == TaskOutcome.SKIPPED
        result.task(":fetchLatest").outcome == TaskOutcome.SKIPPED
        result.task(":checkoutBwcBranch").outcome == TaskOutcome.SKIPPED
    }

    def "git tasks run when no DRA build ID is configured"() {
        when:
        def result = gradleRunner("createClone", '--stacktrace').build()
        then:
        result.task(":createClone").outcome == TaskOutcome.SUCCESS
    }

    def "can resolve checkout folder as project artifact"() {
        given:
        settingsFile << "include ':consumer'"
        file("cloned/consumer/build.gradle") << """
            configurations {
                consumeCheckout
            }
            dependencies {
                consumeCheckout project(path:":", configuration: "checkout")
            }

            tasks.register("register") {
                dependsOn configurations.consumeCheckout
                doLast {
                    configurations.consumeCheckout.files.each {
                        println "checkoutDir artifact: " + it
                    }
                }
            }
        """
        when:
        def result = gradleRunner(":consumer:register", '--stacktrace', "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin").build()
        then:
        result.task(":checkoutBwcBranch").outcome == TaskOutcome.SUCCESS
        result.task(":consumer:register").outcome == TaskOutcome.SUCCESS
        result.output.contains("./build/checkout")
    }
}
