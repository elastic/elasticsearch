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
        execute("git branch origin/7.x", file("cloned"))
        execute("git branch origin/7.10", file("cloned"))
    }

    def "builds distribution from branches via archives assemble"() {
        given:
        buildFile.text = ""
        internalBuild(buildFile, "7.10.1", "7.11.0", "7.12.0")
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-bwc-setup'
        """
        when:
        def result = gradleRunner(":distribution:bwc:${bwcProject}:buildBwcDarwinTar",
                ":distribution:bwc:${bwcProject}:buildBwcOssDarwinTar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin",
                "-Dbwc.dist.version=${bwcDistVersion}-SNAPSHOT")
                .build()
        then:
        result.task(":distribution:bwc:${bwcProject}:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS
        result.task(":distribution:bwc:${bwcProject}:buildBwcOssDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "assemble task triggered"
        assertOutputContains(result.output, "[$bwcDistVersion] > Task :distribution:archives:darwin-tar:${expectedAssembleTaskName}")
        assertOutputContains(result.output, "[$bwcDistVersion] > Task :distribution:archives:oss-darwin-tar:${expectedAssembleTaskName}")

        where:
        bwcDistVersion | bwcProject | expectedAssembleTaskName
        "7.10.1"       | "bugfix"   | "assemble"
    }

    def "builds distribution from branches via archives extractedAssemble"() {
        given:
        buildFile.text = ""
        internalBuild(buildFile, "7.12.1", "7.13.0", "7.14.0")
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-bwc-setup'
        """
        when:
        def result = gradleRunner(":distribution:bwc:${bwcProject}:buildBwcDarwinTar",
                ":distribution:bwc:${bwcProject}:buildBwcOssDarwinTar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin",
                "-Dbwc.dist.version=${bwcDistVersion}-SNAPSHOT")
                .build()
        then:
        result.task(":distribution:bwc:${bwcProject}:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS
        result.task(":distribution:bwc:${bwcProject}:buildBwcOssDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "assemble task triggered"
        assertOutputContains(result.output, "[$bwcDistVersion] > Task :distribution:archives:darwin-tar:${expectedAssembleTaskName}")
        assertOutputContains(result.output, "[$bwcDistVersion] > Task :distribution:archives:oss-darwin-tar:${expectedAssembleTaskName}")

        where:
        bwcDistVersion | bwcProject | expectedAssembleTaskName
        "7.14.0"       | "minor"    | "extractedAssemble"
    }

    @Unroll
    def "supports #platform aarch distributions"() {
        when:
        def result = gradleRunner(":distribution:bwc:minor:buildBwc${platform.capitalize()}Aarch64Tar",
                ":distribution:bwc:minor:buildBwcOss${platform.capitalize()}Aarch64Tar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin",
                "-Dbwc.dist.version=${bwcDistVersion}-SNAPSHOT")
                .build()
        then:
        result.task(":distribution:bwc:minor:buildBwc${platform.capitalize()}Aarch64Tar").outcome == TaskOutcome.SUCCESS
        result.task(":distribution:bwc:minor:buildBwcOss${platform.capitalize()}Aarch64Tar").outcome == TaskOutcome.SUCCESS

        and: "assemble tasks triggered"
        assertOutputContains(result.output, "[$bwcDistVersion] > Task :distribution:archives:${platform}-aarch64-tar:extractedAssemble")
        assertOutputContains(result.output, "[$bwcDistVersion] > Task :distribution:archives:oss-${platform}-aarch64-tar:extractedAssemble")

        where:
        bwcDistVersion | platform
        "7.12.0"       | "darwin"
        "7.12.0"       | "linux"
    }

    def "bwc distribution archives can be resolved as bwc project artifact"() {
        setup:
        buildFile << """

        configurations {
            dists
        }

        dependencies {
            dists project(path: ":distribution:bwc:bugfix", configuration:"darwin-tar")
        }

        tasks.register("resolveDistributionArchive") {
            inputs.files(configurations.dists)
            doLast {
                configurations.dists.files.each {
                    println "distfile " + (it.absolutePath - project.rootDir.absolutePath)
                }
            }
        }
        """
        when:
        def result = gradleRunner(":resolveDistributionArchive",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin")
                .build()
        then:
        result.task(":resolveDistributionArchive").outcome == TaskOutcome.SUCCESS
        result.task(":distribution:bwc:bugfix:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "assemble task triggered"
        result.output.contains("[7.10.1] > Task :distribution:archives:darwin-tar:assemble")
        normalized(result.output)
                .contains("distfile /distribution/bwc/bugfix/build/bwc/checkout-7.10/distribution/archives/darwin-tar/" +
                        "build/distributions/elasticsearch-7.10.1-SNAPSHOT-darwin-x86_64.tar.gz")
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
        result.output.contains("[7.12.0] > Task :distribution:archives:darwin-tar:extractedAssemble")
        normalized(result.output)
                .contains("expandedRootPath /distribution/bwc/minor/build/bwc/checkout-7.x/" +
                        "distribution/archives/darwin-tar/build/install")
        normalized(result.output)
                .contains("nested folder /distribution/bwc/minor/build/bwc/checkout-7.x/" +
                        "distribution/archives/darwin-tar/build/install/elasticsearch-7.12.0-SNAPSHOT")
    }
}
