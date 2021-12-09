/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.Architecture
import org.elasticsearch.gradle.fixtures.AbstractGitAwareGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.IgnoreIf
import spock.lang.Unroll

/*
 * Test is ignored on ARM since this test case tests the ability to build certain older BWC branches that we don't support on ARM
 */

@IgnoreIf({ Architecture.current() == Architecture.AARCH64 })
class InternalDistributionBwcSetupPluginFuncTest extends AbstractGitAwareGradleFuncTest {

    def setup() {
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-bwc-setup'
        """
        execute("git branch origin/8.0", file("cloned"))
        execute("git branch origin/7.16", file("cloned"))
        execute("git branch origin/7.15", file("cloned"))
    }

    def "builds distribution from branches via archives extractedAssemble"() {
        given:
        buildFile.text = ""
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-bwc-setup'
        """
        when:
        def result = gradleRunner(":distribution:bwc:${bwcProject}:buildBwcDarwinTar",
                ":distribution:bwc:${bwcProject}:buildBwcDarwinTar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin",
                "-Dbwc.dist.version=${bwcDistVersion}-SNAPSHOT")
                .build()
        then:
        result.task(":distribution:bwc:${bwcProject}:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "assemble task triggered"
        assertOutputContains(result.output, "[$bwcDistVersion] > Task :distribution:archives:darwin-tar:${expectedAssembleTaskName}")

        where:
        bwcDistVersion | bwcProject | expectedAssembleTaskName
        "8.0.0"        | "minor"    | "extractedAssemble"
        "7.16.0"       | "staged"   | "extractedAssemble"
        "7.15.2"       | "bugfix"   | "extractedAssemble"
    }

    @Unroll
    def "supports #platform aarch distributions"() {
        when:
        def result = gradleRunner(":distribution:bwc:minor:buildBwc${platform.capitalize()}Aarch64Tar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin",
                "-Dbwc.dist.version=${bwcDistVersion}-SNAPSHOT")
                .build()
        then:
        result.task(":distribution:bwc:minor:buildBwc${platform.capitalize()}Aarch64Tar").outcome == TaskOutcome.SUCCESS

        and: "assemble tasks triggered"
        assertOutputContains(result.output, "[$bwcDistVersion] > Task :distribution:archives:${platform}-aarch64-tar:extractedAssemble")

        where:
        bwcDistVersion | platform
        "8.0.0"       | "darwin"
        "8.0.0"       | "linux"
    }

    def "bwc expanded distribution folder can be resolved as bwc project artifact"() {
        setup:
        buildFile << """

        configurations {
            expandedDist
        }

        dependencies {
            expandedDist project(path: ":distribution:bwc:minor", configuration:"expanded-darwin-tar")
        }

        tasks.register("resolveExpandedDistribution") {
            inputs.files(configurations.expandedDist)
            doLast {
                configurations.expandedDist.files.each {
                    println "expandedRootPath " + (it.absolutePath - project.rootDir.absolutePath)
                    it.eachFile { nested ->
                        println "nested folder " + (nested.absolutePath - project.rootDir.absolutePath)
                    }
                }
            }
        }
        """
        when:
        def result = gradleRunner(":resolveExpandedDistribution",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin")
                .build()
        then:
        result.task(":resolveExpandedDistribution").outcome == TaskOutcome.SUCCESS
        result.task(":distribution:bwc:minor:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS
        and: "assemble task triggered"
        result.output.contains("[8.0.0] > Task :distribution:archives:darwin-tar:extractedAssemble")
        result.output.contains("expandedRootPath /distribution/bwc/minor/build/bwc/checkout-8.0/" +
                        "distribution/archives/darwin-tar/build/install")
        result.output.contains("nested folder /distribution/bwc/minor/build/bwc/checkout-8.0/" +
                        "distribution/archives/darwin-tar/build/install/elasticsearch-8.0.0-SNAPSHOT")
    }
}
