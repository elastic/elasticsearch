/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGitAwareGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.Unroll

class InternalDistributionBwcSetupPluginFuncTest extends AbstractGitAwareGradleFuncTest {

    def setup() {
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-bwc-setup'
        """
        execute("git branch origin/7.x", file("cloned"))
        execute("git branch origin/7.9", file("cloned"))
    }

    @Unroll
    def "builds distribution from branches via archives #expectedAssembleTaskName"() {
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
        result.output.contains("[$bwcDistVersion] > Task :distribution:archives:darwin-tar:${expectedAssembleTaskName}\n")
        result.output.contains("[$bwcDistVersion] > Task :distribution:archives:oss-darwin-tar:${expectedAssembleTaskName}\n")

        where:
        bwcDistVersion | bwcProject | expectedAssembleTaskName
        "7.9.1"        | "bugfix"   | "assemble"
        "7.11.0"       | "minor"    | "extractedAssemble"
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
        result.output.contains("[7.9.1] > Task :distribution:archives:darwin-tar:assemble")
        normalized(result.output)
                .contains("distfile /distribution/bwc/bugfix/build/bwc/checkout-7.9/distribution/archives/darwin-tar/" +
                        "build/distributions/elasticsearch-7.9.1-SNAPSHOT-darwin-x86_64.tar.gz")
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
                    println "distfile " + (it.absolutePath - project.rootDir.absolutePath)
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
        result.output.contains("[7.11.0] > Task :distribution:archives:darwin-tar:extractedAssemble")
        normalized(result.output)
                .contains("distfile /distribution/bwc/minor/build/bwc/checkout-7.x/" +
                        "distribution/archives/darwin-tar/build/install")
    }
}
