/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.Architecture
import org.elasticsearch.gradle.DistributionDownloadPlugin
import org.elasticsearch.gradle.DistributionResolution
import org.elasticsearch.gradle.ElasticsearchDistribution
import org.elasticsearch.gradle.ElasticsearchDistributionType
import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.distribution.ElasticsearchDistributionTypes
import org.elasticsearch.gradle.fixtures.AbstractProjectBuilderPluginSpec
import org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes
import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency
import spock.lang.TempDir
import spock.lang.Unroll

class InternalDistributionDownloadPluginSpec extends AbstractProjectBuilderPluginSpec {

    @TempDir
    File workspace

    Project consumer

    @Override
    Class<InternalDistributionDownloadPlugin> getPluginClassUnderTest() {
        return InternalDistributionDownloadPlugin
    }

    def setup() {
        writeRepoLayout()

        Project rootProject = buildProject("elasticsearch", null, workspace)
        Project distributionProject = buildProject("distribution", rootProject)
        Project archivesProject = buildProject("archives", distributionProject)
        Project packagesProject = buildProject("packages", distributionProject)
        Project bwcProject = buildProject("bwc", distributionProject)
        buildProject("linux-tar", archivesProject)
        ["rpm", "no-jdk-rpm", "deb", "no-jdk-deb"].each { String packageProjectName ->
            Project packageProject = buildProject(packageProjectName, packagesProject)
            packageProject.configurations.create("default")
        }
        ["minor1", "main"].each { String bwcProjectName ->
            Project bwcChildProject = buildProject(bwcProjectName, bwcProject)
            bwcChildProject.configurations.create("rpm")
            bwcChildProject.configurations.create("deb")
            bwcChildProject.configurations.create("expanded-linux-tar")
        }

        consumer = buildProject("consumer", rootProject)
        consumer.gradle.startParameter.offline = true
        applyPluginUnderTest(consumer)
    }

    def "applies DistributionDownloadPlugin and registers the internal distribution resolvers"() {
        when:
        List<String> registrations = DistributionDownloadPlugin.getRegistrationsContainer(consumer).collect { it.name }

        then:
        consumer.plugins.hasPlugin(DistributionDownloadPlugin)
        registrations == ["local-build", "bwc", "detached"]
    }

    @Unroll
    def "#resolutionName resolution maps archive distro #version to #expectedPath[#expectedConfiguration]"() {
        given:
        ElasticsearchDistribution distribution = archiveDistribution("testDistro", version, detachedVersion)
        DistributionResolution resolution = DistributionDownloadPlugin.getRegistrationsContainer(consumer).find { it.name == resolutionName }

        when:
        ProjectDependency dependency = (ProjectDependency) resolution.resolver.resolve(consumer, distribution).defaultNotation

        then:
        dependency.path == expectedPath
        dependency.targetConfiguration == expectedConfiguration

        where:
        resolutionName | version                         | detachedVersion | expectedPath                        | expectedConfiguration
        "local-build" | VersionProperties.elasticsearch | false           | ":distribution:archives:linux-tar" | "default"
        "bwc"         | "9.0.3"                       | false           | ":distribution:bwc:minor1"         | "expanded-linux-tar"
        "detached"    | "9.2.0"                       | true            | ":distribution:bwc:main"           | "expanded-linux-tar"
    }

    @Unroll
    def "local-build resolution maps current #type package bundledJdk=#bundledJdk to #expectedPath[default]"() {
        given:
        ElasticsearchDistribution distribution = packageDistribution(
            "testDistro",
            VersionProperties.elasticsearch,
            false,
            type,
            bundledJdk
        )
        DistributionResolution resolution = DistributionDownloadPlugin.getRegistrationsContainer(consumer).find { it.name == "local-build" }

        when:
        ProjectDependency dependency = (ProjectDependency) resolution.resolver.resolve(consumer, distribution).defaultNotation

        then:
        dependency.path == expectedPath
        dependency.targetConfiguration == "default"

        where:
        type                                     | bundledJdk || expectedPath
        InternalElasticsearchDistributionTypes.RPM | true       || ":distribution:packages:rpm"
        InternalElasticsearchDistributionTypes.RPM | false      || ":distribution:packages:no-jdk-rpm"
        InternalElasticsearchDistributionTypes.DEB | true       || ":distribution:packages:deb"
        InternalElasticsearchDistributionTypes.DEB | false      || ":distribution:packages:no-jdk-deb"
    }

    @Unroll
    def "bwc resolution maps package #version #type to #expectedPath[#expectedConfiguration]"() {
        given:
        ElasticsearchDistribution distribution = packageDistribution("testDistro", version, false, type, true)
        DistributionResolution resolution = DistributionDownloadPlugin.getRegistrationsContainer(consumer).find { it.name == "bwc" }

        when:
        ProjectDependency dependency = (ProjectDependency) resolution.resolver.resolve(consumer, distribution).defaultNotation

        then:
        dependency.path == expectedPath
        dependency.targetConfiguration == expectedConfiguration

        where:
        version  | type                                     || expectedPath                | expectedConfiguration
        "9.0.3" | InternalElasticsearchDistributionTypes.RPM || ":distribution:bwc:minor1" | "rpm"
        "9.0.3" | InternalElasticsearchDistributionTypes.DEB || ":distribution:bwc:minor1" | "deb"
    }

    private ElasticsearchDistribution archiveDistribution(String name, String version, boolean detachedVersion) {
        return distribution(name, version, detachedVersion, ElasticsearchDistributionTypes.ARCHIVE, true) {
            it.platform = ElasticsearchDistribution.Platform.LINUX
            it.architecture = Architecture.X64
        }
    }

    private ElasticsearchDistribution packageDistribution(
        String name,
        String version,
        boolean detachedVersion,
        ElasticsearchDistributionType type,
        boolean bundledJdk
    ) {
        return distribution(name, version, detachedVersion, type, bundledJdk) {
            it.architecture = Architecture.X64
        }
    }

    private ElasticsearchDistribution distribution(
        String name,
        String version,
        boolean detachedVersion,
        ElasticsearchDistributionType type,
        boolean bundledJdk,
        Closure<?> configure = {}
    ) {
        return DistributionDownloadPlugin.getContainer(consumer).create(name) {
            it.version = version
            it.type = type
            it.detachedVersion = detachedVersion
            it.bundledJdk = bundledJdk
            configure.call(it)
        }.maybeFreeze()
    }

    private void writeRepoLayout() {
        new File(workspace, ".ci").mkdirs()
        new File(workspace, ".ci/dockerOnLinuxExclusions").text = ""

        writeBuildToolsVersionProperties(workspace)
        new File(workspace, "branches.json").text = """
            {
              "branches": [
                { "branch": "main", "version": "9.1.0" },
                { "branch": "9.0", "version": "9.0.3" },
                { "branch": "8.19", "version": "8.19.1" }
              ]
            }
        """

        writeVersionJava(workspace)
    }
}
