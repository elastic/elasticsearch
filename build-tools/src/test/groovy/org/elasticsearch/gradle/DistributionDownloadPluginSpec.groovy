/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle

import org.gradle.api.NamedDomainObjectContainer
import org.gradle.api.Project
import org.gradle.testfixtures.ProjectBuilder
import spock.lang.Specification

import static org.elasticsearch.gradle.ElasticsearchDistribution.CURRENT_PLATFORM
import static org.elasticsearch.gradle.ElasticsearchDistribution.Platform.LINUX
import static org.elasticsearch.gradle.distribution.ElasticsearchDistributionTypes.ARCHIVE
import static org.elasticsearch.gradle.distribution.ElasticsearchDistributionTypes.INTEG_TEST_ZIP

class DistributionDownloadPluginSpec extends Specification {

    private Project project

    def setup() {
        Project rootProject = ProjectBuilder.builder().build()
        project = ProjectBuilder.builder().withParent(rootProject).build()
        project.plugins.apply("elasticsearch.distribution-download")
    }

    def "version default provides"() {
        when:
        def distro = distro(project, "testdistro", null, ARCHIVE, LINUX, true)
        then:
        distro.version == VersionProperties.elasticsearch
    }

    def "bad version format is handled"() {
        when:
        distro(project, "testdistro", "badversion", ARCHIVE, LINUX, true)
        then:
        def e = thrown(IllegalArgumentException)
        e.message == "Invalid version format: 'badversion'. Should be major.minor.revision[-(alpha|beta|rc)Number|-SNAPSHOT]"
    }

    def "type default provided"() {
        when:
        def distro = distro(project, "testdistro", "5.0.0", null, LINUX, true)
        then:
        distro.type == ARCHIVE
    }

    def "platform default provided"() {
        when:
        def distro = distro(project, "testdistro", "5.0.0", ARCHIVE, null, true)
        then:
        distro.platform == CURRENT_PLATFORM
    }

    def "platform for integTest is not configurable"() {
        when:
        distro(project, "testdistro", "5.0.0", INTEG_TEST_ZIP, LINUX, null)
        then:
        def e = thrown(IllegalArgumentException)
        e.message == "platform cannot be set on elasticsearch distribution [testdistro] of type [integ_test_zip]"
    }

    def "bundled jdk default provided"() {
        when:
        def distro = distro(project, "testdistro", "5.0.0", ARCHIVE, LINUX, true)
        then:
        distro.bundledJdk
    }

    def "bundled jdk for integTest is not configurable"() {
        when:
        distro(project,  "testdistro", "5.0.0", INTEG_TEST_ZIP, null, true)
        then:
        def e = thrown(IllegalArgumentException)
        e.message == "bundledJdk cannot be set on elasticsearch distribution [testdistro] of type [integ_test_zip]"
    }

    // create a distro and finalize its configuration
    private ElasticsearchDistribution distro(
            Project project,
            String name,
            String version,
            ElasticsearchDistributionType type,
            ElasticsearchDistribution.Platform platform,
            Boolean bundledJdk
    ) {
        NamedDomainObjectContainer<ElasticsearchDistribution> distros = DistributionDownloadPlugin.getContainer(project)
        ElasticsearchDistribution distribution = distros.create(name, distro -> {
            if (version != null) {
                distro.setVersion(version)
            }
            if (type != null) {
                distro.setType(type)
            }
            if (platform != null) {
                distro.setPlatform(platform)
            }
            if (bundledJdk != null) {
                distro.setBundledJdk(bundledJdk)
            }
        }).maybeFreeze()
        distribution.finalizeValues()
        return distribution
    }
}
