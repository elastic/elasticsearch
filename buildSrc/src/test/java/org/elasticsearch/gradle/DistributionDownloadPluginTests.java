/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.elasticsearch.gradle.ElasticsearchDistribution.Platform;
import org.elasticsearch.gradle.ElasticsearchDistribution.Type;
import org.elasticsearch.gradle.internal.VersionProperties;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.internal.BwcVersions;
import org.elasticsearch.gradle.internal.test.GradleUnitTestCase;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

import java.io.File;
import java.util.Arrays;
import java.util.TreeSet;

import static org.hamcrest.core.StringContains.containsString;

public class DistributionDownloadPluginTests extends GradleUnitTestCase {
    private static Project rootProject;
    private static Project archivesProject;
    private static Project packagesProject;
    private static Project bwcProject;

    private static final Version BWC_MAJOR_VERSION = Version.fromString("2.0.0");
    private static final Version BWC_MINOR_VERSION = Version.fromString("1.1.0");
    private static final Version BWC_STAGED_VERSION = Version.fromString("1.0.0");
    private static final Version BWC_BUGFIX_VERSION = Version.fromString("1.0.1");
    private static final Version BWC_MAINTENANCE_VERSION = Version.fromString("0.90.1");
    private static final BwcVersions BWC_MINOR = new BwcVersions(
        new TreeSet<>(Arrays.asList(BWC_BUGFIX_VERSION, BWC_MINOR_VERSION, BWC_MAJOR_VERSION)),
        BWC_MAJOR_VERSION
    );
    private static final BwcVersions BWC_STAGED = new BwcVersions(
        new TreeSet<>(Arrays.asList(BWC_STAGED_VERSION, BWC_MINOR_VERSION, BWC_MAJOR_VERSION)),
        BWC_MAJOR_VERSION
    );
    private static final BwcVersions BWC_BUGFIX = new BwcVersions(
        new TreeSet<>(Arrays.asList(BWC_BUGFIX_VERSION, BWC_MINOR_VERSION, BWC_MAJOR_VERSION)),
        BWC_MAJOR_VERSION
    );
    private static final BwcVersions BWC_MAINTENANCE = new BwcVersions(
        new TreeSet<>(Arrays.asList(BWC_MAINTENANCE_VERSION, BWC_STAGED_VERSION, BWC_MINOR_VERSION)),
        BWC_MINOR_VERSION
    );

    public void testVersionDefault() {
        ElasticsearchDistribution distro = checkDistro(createProject(null, false), "testdistro", null, Type.ARCHIVE, Platform.LINUX, true);
        assertEquals(distro.getVersion(), VersionProperties.getElasticsearch());
    }

    public void testBadVersionFormat() {
        assertDistroError(
            createProject(null, false),
            "testdistro",
            "badversion",
            Type.ARCHIVE,
            Platform.LINUX,
            true,
            "Invalid version format: 'badversion'"
        );
    }

    public void testTypeDefault() {
        ElasticsearchDistribution distro = checkDistro(createProject(null, false), "testdistro", "5.0.0", null, Platform.LINUX, true);
        assertEquals(distro.getType(), Type.ARCHIVE);
    }

    public void testPlatformDefault() {
        ElasticsearchDistribution distro = checkDistro(createProject(null, false), "testdistro", "5.0.0", Type.ARCHIVE, null, true);
        assertEquals(distro.getPlatform(), ElasticsearchDistribution.CURRENT_PLATFORM);
    }

    public void testPlatformForIntegTest() {
        assertDistroError(
            createProject(null, false),
            "testdistro",
            "5.0.0",
            Type.INTEG_TEST_ZIP,
            Platform.LINUX,
            null,
            "platform cannot be set on elasticsearch distribution [testdistro]"
        );
    }

    public void testBundledJdkDefault() {
        ElasticsearchDistribution distro = checkDistro(
            createProject(null, false),
            "testdistro",
            "5.0.0",
            Type.ARCHIVE,
            Platform.LINUX,
            true
        );
        assertTrue(distro.getBundledJdk());
    }

    public void testBundledJdkForIntegTest() {
        assertDistroError(
            createProject(null, false),
            "testdistro",
            "5.0.0",
            Type.INTEG_TEST_ZIP,
            null,
            true,
            "bundledJdk cannot be set on elasticsearch distribution [testdistro]"
        );
    }

    public void testLocalCurrentVersionIntegTestZip() {
        Project project = createProject(BWC_MINOR, true);
        Project archiveProject = ProjectBuilder.builder().withParent(archivesProject).withName("integ-test-zip").build();
        archiveProject.getConfigurations().create("default");
        archiveProject.getArtifacts().add("default", new File("doesnotmatter"));
        createDistro(project, "distro", VersionProperties.getElasticsearch(), Type.INTEG_TEST_ZIP, null, null);
    }

    public void testLocalCurrentVersionArchives() {
        for (Platform platform : Platform.values()) {
            for (boolean bundledJdk : new boolean[] { true, false }) {
                // create a new project in each iteration, so that we know we are resolving the only additional project being created
                Project project = createProject(BWC_MINOR, true);
                String projectName = projectName(platform.toString(), bundledJdk);
                projectName += (platform == Platform.WINDOWS ? "-zip" : "-tar");
                Project archiveProject = ProjectBuilder.builder().withParent(archivesProject).withName(projectName).build();
                archiveProject.getConfigurations().create("default");
                archiveProject.getArtifacts().add("default", new File("doesnotmatter"));
                createDistro(project, "distro", VersionProperties.getElasticsearch(), Type.ARCHIVE, platform, bundledJdk);
            }
        }
    }

    public void testLocalCurrentVersionPackages() {
        for (Type packageType : new Type[] { Type.RPM, Type.DEB }) {
            for (boolean bundledJdk : new boolean[] { true, false }) {
                Project project = createProject(BWC_MINOR, true);
                String projectName = projectName(packageType.toString(), bundledJdk);
                Project packageProject = ProjectBuilder.builder().withParent(packagesProject).withName(projectName).build();
                packageProject.getConfigurations().create("default");
                packageProject.getArtifacts().add("default", new File("doesnotmatter"));
                createDistro(project, "distro", VersionProperties.getElasticsearch(), packageType, null, bundledJdk);
            }
        }
    }

    public void testLocalBwcArchives() {
        for (Platform platform : Platform.values()) {
            // note: no non bundled jdk for bwc
            String configName = projectName(platform.toString(), true);
            configName += (platform == Platform.WINDOWS ? "-zip" : "-tar");

            checkBwc("minor", configName, BWC_MINOR_VERSION, Type.ARCHIVE, platform, BWC_MINOR, true);
            checkBwc("staged", configName, BWC_STAGED_VERSION, Type.ARCHIVE, platform, BWC_STAGED, true);
            checkBwc("bugfix", configName, BWC_BUGFIX_VERSION, Type.ARCHIVE, platform, BWC_BUGFIX, true);
            checkBwc("maintenance", configName, BWC_MAINTENANCE_VERSION, Type.ARCHIVE, platform, BWC_MAINTENANCE, true);
        }
    }

    public void testLocalBwcPackages() {
        for (Type packageType : new Type[] { Type.RPM, Type.DEB }) {
            // note: no non bundled jdk for bwc
            String configName = projectName(packageType.toString(), true);
            checkBwc("minor", configName, BWC_MINOR_VERSION, packageType, null, BWC_MINOR, true);
            checkBwc("staged", configName, BWC_STAGED_VERSION, packageType, null, BWC_STAGED, true);
            checkBwc("bugfix", configName, BWC_BUGFIX_VERSION, packageType, null, BWC_BUGFIX, true);
            checkBwc("maintenance", configName, BWC_MAINTENANCE_VERSION, packageType, null, BWC_MAINTENANCE, true);
        }
    }

    private void assertDistroError(
        Project project,
        String name,
        String version,
        Type type,
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

    private ElasticsearchDistribution createDistro(
        Project project,
        String name,
        String version,
        Type type,
        Platform platform,
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

    // create a distro and finalize its configuration
    private ElasticsearchDistribution checkDistro(
        Project project,
        String name,
        String version,
        Type type,
        Platform platform,
        Boolean bundledJdk
    ) {
        ElasticsearchDistribution distribution = createDistro(project, name, version, type, platform, bundledJdk);
        distribution.finalizeValues();
        return distribution;
    }

    private void checkBwc(
        String projectName,
        String config,
        Version version,
        Type type,
        Platform platform,
        BwcVersions bwcVersions,
        boolean isInternal
    ) {
        Project project = createProject(bwcVersions, isInternal);
        Project archiveProject = ProjectBuilder.builder().withParent(bwcProject).withName(projectName).build();
        archiveProject.getConfigurations().create(config);
        archiveProject.getArtifacts().add(config, new File("doesnotmatter"));
        createDistro(project, "distro", version.toString(), type, platform, true);
    }

    private Project createProject(BwcVersions bwcVersions, boolean isInternal) {
        rootProject = ProjectBuilder.builder().build();
        BuildParams.init(params -> params.setIsInternal(isInternal));
        Project distributionProject = ProjectBuilder.builder().withParent(rootProject).withName("distribution").build();
        archivesProject = ProjectBuilder.builder().withParent(distributionProject).withName("archives").build();
        packagesProject = ProjectBuilder.builder().withParent(distributionProject).withName("packages").build();
        bwcProject = ProjectBuilder.builder().withParent(distributionProject).withName("bwc").build();
        Project project = ProjectBuilder.builder().withParent(rootProject).build();
        if (bwcVersions != null) {
            project.getExtensions().getExtraProperties().set("bwcVersions", bwcVersions);
        }
        project.getPlugins().apply("elasticsearch.distribution-download");
        return project;
    }

    private static String projectName(String base, boolean bundledJdk) {
        String prefix = bundledJdk == false ? "no-jdk-" : "";
        return prefix + base;
    }
}
