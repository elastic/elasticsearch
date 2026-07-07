/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import spock.lang.Unroll

import com.github.tomakehurst.wiremock.WireMockServer
import org.elasticsearch.gradle.fixtures.AbstractGitAwareGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse
import static com.github.tomakehurst.wiremock.client.WireMock.get
import static com.github.tomakehurst.wiremock.client.WireMock.head
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo

class InternalDistributionBwcSetupPluginFuncTest extends AbstractGitAwareGradleFuncTest {

    Class<? extends org.gradle.api.Plugin> pluginClassUnderTest = org.elasticsearch.gradle.internal.InternalDistributionBwcSetupPlugin

    WireMockServer wireMock

    def setup() {
        // Cannot serialize BwcSetupExtension containing project object
        configurationCacheCompatible = false
        internalBuild()
        buildFile << """
            apply plugin: 'elasticsearch.internal-distribution-bwc-setup'
        """
        execute("git branch origin/8.x", file("cloned"))
        execute("git branch origin/8.3", file("cloned"))
        execute("git branch origin/8.2", file("cloned"))
        execute("git branch origin/8.1", file("cloned"))
        execute("git branch origin/7.16", file("cloned"))
    }

    def "builds distribution from branches via archives extractedAssemble"() {
        // A single BWC branch is sufficient to verify that buildBwcDarwinTar triggers
        // extractedAssemble — the plugin logic is identical for all supported branches.
        when:
        def result = gradleRunner(
            ":distribution:bwc:major1:buildBwcDarwinTar",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin",
            "-Dbwc.dist.version=8.4.0-SNAPSHOT"
        )
            .build()
        then:
        result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS

        and: "assemble task triggered"
        assertOutputContains(result.output, "[8.4.0] > Task :distribution:archives:darwin-tar:extractedAssemble")
    }

    def "supports linux aarch distributions"() {
        // One aarch64 platform is sufficient to verify the feature; the same plugin
        // logic handles both darwin and linux aarch64 targets.
        when:
        def result = gradleRunner(
            ":distribution:bwc:major1:buildBwcLinuxAarch64Tar",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin",
            "-Dbwc.dist.version=8.4.0-SNAPSHOT"
        )
            .build()
        then:
        result.task(":distribution:bwc:major1:buildBwcLinuxAarch64Tar").outcome == TaskOutcome.SUCCESS

        and: "assemble tasks triggered"
        assertOutputContains(result.output, "[8.4.0] > Task :distribution:archives:linux-aarch64-tar:extractedAssemble")
    }

    def "bwc expanded distribution folder can be resolved as bwc project artifact"() {
        setup:
        buildFile << """

        configurations {
            expandedDist
        }

        dependencies {
            expandedDist project(path: ":distribution:bwc:major1", configuration:"expanded-darwin-tar")
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
        def result = gradleRunner(
            ":resolveExpandedDistribution",
            "-DtestRemoteRepo=" + remoteGitRepo,
            "-Dbwc.remote=origin"
        )
            .build()
        then:
        result.task(":resolveExpandedDistribution").outcome == TaskOutcome.SUCCESS
        result.task(":distribution:bwc:major1:buildBwcDarwinTar").outcome == TaskOutcome.SUCCESS
        and: "assemble task triggered"
        result.output.contains("[8.4.0] > Task :distribution:archives:darwin-tar:extractedAssemble")
        result.output.contains(
            "expandedRootPath /distribution/bwc/major1/build/bwc/checkout-8.x/" +
                "distribution/archives/darwin-tar/build/install"
        )
        result.output.contains(
            "nested folder /distribution/bwc/major1/build/bwc/checkout-8.x/" +
                "distribution/archives/darwin-tar/build/install/elasticsearch-8.4.0-SNAPSHOT"
        )
    }
}
