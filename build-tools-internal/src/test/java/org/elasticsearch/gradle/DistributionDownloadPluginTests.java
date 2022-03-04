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
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

import java.io.File;

import static org.hamcrest.core.StringContains.containsString;

public class DistributionDownloadPluginTests extends AbstractDistributionDownloadPluginTests {

    public void testVersionDefault() {
        ElasticsearchDistribution distro = checkDistro(
            createProject(null),
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
            createProject(null),
            "testdistro",
            "badversion",
            ElasticsearchDistributionTypes.ARCHIVE,
            Platform.LINUX,
            true,
            "Invalid version format: 'badversion'"
        );
    }

    public void testTypeDefault() {
        ElasticsearchDistribution distro = checkDistro(createProject(null), "testdistro", "5.0.0", null, Platform.LINUX, true);
        assertEquals(distro.getType(), ElasticsearchDistributionTypes.ARCHIVE);
    }

    public void testPlatformDefault() {
        ElasticsearchDistribution distro = checkDistro(
            createProject(null),
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
            createProject(null),
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
            createProject(null),
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
            createProject(null),
            "testdistro",
            "5.0.0",
            ElasticsearchDistributionTypes.INTEG_TEST_ZIP,
            null,
            true,
            "bundledJdk cannot be set on elasticsearch distribution [testdistro]"
        );
    }

    public void testLocalCurrentVersionIntegTestZip() {
        Project project = createProject(BWC_MINOR);
        Project archiveProject = ProjectBuilder.builder().withParent(archivesProject).withName("integ-test-zip").build();
        archiveProject.getConfigurations().create("default");
        archiveProject.getArtifacts().add("default", new File("doesnotmatter"));
        createDistro(project, "distro", VersionProperties.getElasticsearch(), ElasticsearchDistributionTypes.INTEG_TEST_ZIP, null, null);
    }

    public void testLocalCurrentVersionArchives() {
        for (Platform platform : Platform.values()) {
            for (boolean bundledJdk : new boolean[] { true, false }) {
                // create a new project in each iteration, so that we know we are resolving the only additional project being created
                Project project = createProject(BWC_MINOR);
                String projectName = projectName(platform.toString(), bundledJdk);
                projectName += (platform == Platform.WINDOWS ? "-zip" : "-tar");
                Project archiveProject = ProjectBuilder.builder().withParent(archivesProject).withName(projectName).build();
                archiveProject.getConfigurations().create("default");
                archiveProject.getArtifacts().add("default", new File("doesnotmatter"));
                createDistro(
                    project,
                    "distro",
                    VersionProperties.getElasticsearch(),
                    ElasticsearchDistributionTypes.ARCHIVE,
                    platform,
                    bundledJdk
                );
            }
        }
    }

    public void testLocalBwcArchives() {
        for (Platform platform : Platform.values()) {
            // note: no non bundled jdk for bwc
            String configName = projectName(platform.toString(), true);
            configName += (platform == Platform.WINDOWS ? "-zip" : "-tar");
            ElasticsearchDistributionType archiveType = ElasticsearchDistributionTypes.ARCHIVE;
            checkBwc("minor", configName, BWC_MINOR_VERSION.elasticsearch(), archiveType, platform, BWC_MINOR);
            checkBwc("staged", configName, BWC_STAGED_VERSION.elasticsearch(), archiveType, platform, BWC_STAGED);
            checkBwc("bugfix", configName, BWC_BUGFIX_VERSION.elasticsearch(), archiveType, platform, BWC_BUGFIX);
            checkBwc("maintenance", configName, BWC_MAINTENANCE_VERSION.elasticsearch(), archiveType, platform, BWC_MAINTENANCE);
        }
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

}
