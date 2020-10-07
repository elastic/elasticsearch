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

import org.apache.commons.io.FileUtils
import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import org.junit.Rule
import org.junit.rules.TemporaryFolder

class InternalDistributionBwcSetupPluginFuncTest extends AbstractGradleFuncTest {

    @Rule
    TemporaryFolder remoteRepoDirs = new TemporaryFolder()

    File remoteGitRepo

    def setup() {
        remoteGitRepo = new File(setupGitRemote(), '.git')

        "git clone ${remoteGitRepo.absolutePath}".execute(Collections.emptyList(), testProjectDir.root).waitFor()
        File buildScript = new File(testProjectDir.root, 'remote/build.gradle')
        internalBuild(buildScript)
        buildScript << """
            apply plugin: 'elasticsearch.internal-distribution-bwc-setup'
        """
    }

    def "builds distribution from branches via archives assemble"() {
        when:
        def result = gradleRunner(new File(testProjectDir.root, "remote"),
                ":distribution:bwc:bugfix:buildBwcDarwinTar",
                ":distribution:bwc:bugfix:buildBwcOssDarwinTar",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin")
                .build()
        then:
        result.task(":distribution:bwc:bugfix:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS
        result.task(":distribution:bwc:bugfix:buildBwcOssDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "assemble task triggered"
        result.output.contains("[8.0.1] > Task :distribution:archives:darwin-tar:assemble")
        result.output.contains("[8.0.1] > Task :distribution:archives:oss-darwin-tar:assemble")
    }

    def "bwc distribution archives can be resolved as bwc project artifact"() {
        setup:
        new File(testProjectDir.root, 'remote/build.gradle') << """
        
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
        def result = gradleRunner(new File(testProjectDir.root, "remote"),
                ":resolveDistributionArchive",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin")
                .build()
        then:
        result.task(":resolveDistributionArchive").outcome == TaskOutcome.SUCCESS
        result.task(":distribution:bwc:bugfix:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "assemble task triggered"
        result.output.contains("[8.0.1] > Task :distribution:archives:darwin-tar:assemble")
        normalizedOutput(result.output)
                .contains("distfile /distribution/bwc/bugfix/build/bwc/checkout-8.0/distribution/archives/darwin-tar/" +
                        "build/distributions/elasticsearch-8.0.1-SNAPSHOT-darwin-x86_64.tar.gz")
    }

    def "bwc expanded distribution folder can be resolved as bwc project artifact"() {
        setup:
        new File(testProjectDir.root, 'remote/build.gradle') << """
        
        configurations {
            expandedDist
        }
        
        dependencies {
            expandedDist project(path: ":distribution:bwc:bugfix", configuration:"expanded-darwin-tar")
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
        def result = gradleRunner(new File(testProjectDir.root, "remote"),
                ":resolveExpandedDistribution",
                "-DtestRemoteRepo=" + remoteGitRepo,
                "-Dbwc.remote=origin")
                .build()
        then:
        result.task(":resolveExpandedDistribution").outcome == TaskOutcome.SUCCESS
        result.task(":distribution:bwc:bugfix:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "assemble task triggered"
        result.output.contains("[8.0.1] > Task :distribution:archives:darwin-tar:assemble")
        normalizedOutput(result.output)
                .contains("distfile /distribution/bwc/bugfix/build/bwc/checkout-8.0/" +
                        "distribution/archives/darwin-tar/build/install")
    }

    File setupGitRemote() {
        URL fakeRemote = getClass().getResource("fake_git/remote")
        File workingRemoteGit = new File(remoteRepoDirs.root, 'remote')
        FileUtils.copyDirectory(new File(fakeRemote.file), workingRemoteGit)
        fakeRemote.file + "/.git"
        gradleRunner(workingRemoteGit, "wrapper").build()
        execute("git init", workingRemoteGit)
        execute("git add .", workingRemoteGit)
        execute('git commit -m"Initial"', workingRemoteGit)
        execute("git checkout -b origin/8.0", workingRemoteGit)
        return workingRemoteGit;
    }
}
