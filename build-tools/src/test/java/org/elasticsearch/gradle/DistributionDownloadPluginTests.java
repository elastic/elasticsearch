/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.elasticsearch.gradle.ElasticsearchDistribution.Platform;
import org.elasticsearch.gradle.distribution.ElasticsearchDistributionTypes;
import org.elasticsearch.gradle.internal.test.GradleUnitTestCase;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

import static org.hamcrest.core.StringContains.containsString;

public class DistributionDownloadPluginTests extends GradleUnitTestCase {

    protected static Project rootProject;

    public void testVersionDefault() {
        ElasticsearchDistribution distro = checkDistro(
            createProject(),
            "testdistro",
            null,
            ElasticsearchDistributionTypes.ARCHIVE,
            Platform.LINUX,
            true
        );
        assertEquals(distro.getVersion(), VersionProperties.getElasticsearch());
    }

    public void testBadVersionFormat() {
        assertDistroError(
            createProject(),
            "testdistro",
            "badversion",
            ElasticsearchDistributionTypes.ARCHIVE,
            Platform.LINUX,
            true,
            "Invalid version format: 'badversion'"
        );
    }

    public void testTypeDefault() {
        ElasticsearchDistribution distro = checkDistro(createProject(), "testdistro", "5.0.0", null, Platform.LINUX, true);
        assertEquals(distro.getType(), ElasticsearchDistributionTypes.ARCHIVE);
    }

    public void testPlatformDefault() {
        ElasticsearchDistribution distro = checkDistro(
            createProject(),
            "testdistro",
            "5.0.0",
            ElasticsearchDistributionTypes.ARCHIVE,
            null,
            true
        );
        assertEquals(distro.getPlatform(), ElasticsearchDistribution.CURRENT_PLATFORM);
    }

    public void testPlatformForIntegTest() {
        assertDistroError(
            createProject(),
            "testdistro",
            "5.0.0",
            ElasticsearchDistributionTypes.INTEG_TEST_ZIP,
            Platform.LINUX,
            null,
            "platform cannot be set on elasticsearch distribution [testdistro]"
        );
    }

    public void testBundledJdkDefault() {
        ElasticsearchDistribution distro = checkDistro(
            createProject(),
            "testdistro",
            "5.0.0",
            ElasticsearchDistributionTypes.ARCHIVE,
            Platform.LINUX,
            true
        );
        assertTrue(distro.getBundledJdk());
    }

    public void testBundledJdkForIntegTest() {
        assertDistroError(
            createProject(),
            "testdistro",
            "5.0.0",
            ElasticsearchDistributionTypes.INTEG_TEST_ZIP,
            null,
            true,
            "bundledJdk cannot be set on elasticsearch distribution [testdistro]"
        );
    }

    private void assertDistroError(
        Project project,
        String name,
        String version,
        ElasticsearchDistributionType type,
        Platform platform,
        Boolean bundledJdk,
        String message
    ) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> checkDistro(project, name, version, type, platform, bundledJdk)
        );
        assertThat(e.getMessage(), containsString(message));
    }

    // create a distro and finalize its configuration
    private ElasticsearchDistribution checkDistro(
        Project project,
        String name,
        String version,
        ElasticsearchDistributionType type,
        Platform platform,
        Boolean bundledJdk
    ) {
        ElasticsearchDistribution distribution = createDistro(project, name, version, type, platform, bundledJdk);
        distribution.finalizeValues();
        return distribution;
    }

    protected ElasticsearchDistribution createDistro(
        Project project,
        String name,
        String version,
        ElasticsearchDistributionType type,
        ElasticsearchDistribution.Platform platform,
        Boolean bundledJdk
    ) {
        NamedDomainObjectContainer<ElasticsearchDistribution> distros = DistributionDownloadPlugin.getContainer(project);
        return distros.create(name, distro -> {
            if (version != null) {
                distro.setVersion(version);
            }
            if (type != null) {
                distro.setType(type);
            }
            if (platform != null) {
                distro.setPlatform(platform);
            }
            if (bundledJdk != null) {
                distro.setBundledJdk(bundledJdk);
            }
        }).maybeFreeze();
    }

    protected Project createProject() {
        rootProject = ProjectBuilder.builder().build();
        // Project distributionProject = ProjectBuilder.builder().withParent(rootProject).withName("distribution").build();
        // archivesProject = ProjectBuilder.builder().withParent(distributionProject).withName("archives").build();
        // packagesProject = ProjectBuilder.builder().withParent(distributionProject).withName("packages").build();
        // bwcProject = ProjectBuilder.builder().withParent(distributionProject).withName("bwc").build();
        Project project = ProjectBuilder.builder().withParent(rootProject).build();
        project.getPlugins().apply("elasticsearch.distribution-download");
        return project;
    }

}
