/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.info

import groovy.json.JsonOutput
import org.elasticsearch.gradle.fixtures.AbstractProjectBuilderPluginSpec
import spock.lang.TempDir

import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.internal.BwcVersions
import org.gradle.api.Project
import org.gradle.api.provider.Provider
import org.gradle.api.provider.ProviderFactory

import java.nio.file.Path

class GlobalBuildInfoPluginSpec extends AbstractProjectBuilderPluginSpec {

    @Override
    Class<GlobalBuildInfoPlugin> getPluginClassUnderTest() {
        return GlobalBuildInfoPlugin
    }

    @TempDir
    File projectRoot

    Project project

    def setup() {
        project = buildProject("bwcTestProject", null, projectRoot)
        project = Spy(project)
        project.getRootProject() >> project

        writeMinimalElasticsearchRepoLayout(projectRoot)
    }

    def "resolve unreleased versions from branches file set by Gradle property"() {
        given:
        ProviderFactory providerFactorySpy = Spy(project.getProviders())
        Path branchesJsonPath = projectRoot.toPath().resolve("myBranches.json")
        Provider<String> gradleBranchesLocationProvider = project.providers.provider { return branchesJsonPath.toString() }
        providerFactorySpy.gradleProperty("org.elasticsearch.build.branches-file-location") >> gradleBranchesLocationProvider
        project.getProviders() >> providerFactorySpy
        branchesJsonPath.text = branchesJson(
            [
                new DevelopmentBranch("main", Version.fromString("9.1.0")),
                new DevelopmentBranch("9.0", Version.fromString("9.0.3")),
                new DevelopmentBranch("8.19", Version.fromString("8.19.1")),
                new DevelopmentBranch("8.18", Version.fromString("8.18.2")),
            ]
        )

        when:
        project.objects.newInstance(getPluginClassUnderTest()).apply(project)
        BuildParameterExtension ext = project.extensions.getByType(BuildParameterExtension)
        BwcVersions bwcVersions = ext.bwcVersions

        then:
        bwcVersions != null
        bwcVersions.unreleased.toSet() == ["9.1.0", "9.0.3", "8.19.1", "8.18.2"].collect { Version.fromString(it) }.toSet()
    }

    def "offline mode still uses configured http(s) branches location when a workspace branches.json exists"() {
        given:
        project.getGradle().getStartParameter().setOffline(true)

        ProviderFactory providerFactorySpy = Spy(project.getProviders())
        Provider<String> gradleBranchesLocationProvider = project.providers.provider { return "https://example.invalid/branches.json" }
        providerFactorySpy.gradleProperty("org.elasticsearch.build.branches-file-location") >> gradleBranchesLocationProvider
        project.getProviders() >> providerFactorySpy

        Path workspaceBranchesJsonPath = projectRoot.toPath().resolve("branches.json")
        workspaceBranchesJsonPath.text = branchesJson(
            [
                new DevelopmentBranch("main", Version.fromString("9.1.0")),
                new DevelopmentBranch("9.0", Version.fromString("9.0.3")),
            ]
        )

        when:
        project.objects.newInstance(getPluginClassUnderTest()).apply(project)
        project.extensions.getByType(BuildParameterExtension).bwcVersions

        then:
        def ex = thrown(UncheckedIOException)
        ex.message == "Failed to download branches.json from: https://example.invalid/branches.json"
        ex.cause instanceof java.net.UnknownHostException
    }

    def "offline mode reports download failure when configured location is an http(s) URL and workspace branches.json is missing"() {
        given:
        project.getGradle().getStartParameter().setOffline(true)

        ProviderFactory providerFactorySpy = Spy(project.getProviders())
        Provider<String> gradleBranchesLocationProvider = project.providers.provider { return "https://example.invalid/branches.json" }
        providerFactorySpy.gradleProperty("org.elasticsearch.build.branches-file-location") >> gradleBranchesLocationProvider
        project.getProviders() >> providerFactorySpy

        when:
        project.objects.newInstance(getPluginClassUnderTest()).apply(project)
        project.extensions.getByType(BuildParameterExtension).bwcVersions

        then:
        def ex = thrown(UncheckedIOException)
        ex.message == "Failed to download branches.json from: https://example.invalid/branches.json"
        ex.cause instanceof java.net.UnknownHostException
    }

    String branchesJson(List<DevelopmentBranch> branches) {
        Map<String, Object> branchesFileContent = [
            branches: branches.collect { branch ->
                [
                    branch : branch.name(),
                    version: branch.version().toString(),
                ]
            }
        ]
        return JsonOutput.prettyPrint(JsonOutput.toJson(branchesFileContent))
    }
}
