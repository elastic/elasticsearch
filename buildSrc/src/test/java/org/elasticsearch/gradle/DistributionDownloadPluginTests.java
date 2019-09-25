/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle;

import org.elasticsearch.gradle.ElasticsearchDistribution.Flavor;
import org.elasticsearch.gradle.ElasticsearchDistribution.Platform;
import org.elasticsearch.gradle.ElasticsearchDistribution.Type;
import org.elasticsearch.gradle.test.GradleUnitTestCase;
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
    private static final BwcVersions BWC_MINOR =
        new BwcVersions(new TreeSet<>(Arrays.asList(BWC_BUGFIX_VERSION, BWC_MINOR_VERSION, BWC_MAJOR_VERSION)), BWC_MAJOR_VERSION);
    private static final BwcVersions BWC_STAGED =
        new BwcVersions(new TreeSet<>(Arrays.asList(BWC_STAGED_VERSION, BWC_MINOR_VERSION, BWC_MAJOR_VERSION)), BWC_MAJOR_VERSION);
    private static final BwcVersions BWC_BUGFIX =
        new BwcVersions(new TreeSet<>(Arrays.asList(BWC_BUGFIX_VERSION, BWC_MINOR_VERSION, BWC_MAJOR_VERSION)), BWC_MAJOR_VERSION);
    private static final BwcVersions BWC_MAINTENANCE =
        new BwcVersions(new TreeSet<>(Arrays.asList(BWC_MAINTENANCE_VERSION, BWC_STAGED_VERSION, BWC_MINOR_VERSION)), BWC_MINOR_VERSION);

    public void testVersionDefault() {
        ElasticsearchDistribution distro = checkDistro(createProject(null),
            "testdistro", null, Type.ARCHIVE, Platform.LINUX, Flavor.OSS, true);
        assertEquals(distro.getVersion(), Version.fromString(VersionProperties.getElasticsearch()));
    }

    public void testBadVersionFormat() {
        assertDistroError(createProject(null), "testdistro", "badversion", Type.ARCHIVE, Platform.LINUX, Flavor.OSS, true,
            "Invalid version format: 'badversion'");
    }

    public void testTypeDefault() {
        ElasticsearchDistribution distro = checkDistro(createProject(null),
            "testdistro", "5.0.0", null, Platform.LINUX, Flavor.OSS, true);
        assertEquals(distro.getType(), Type.ARCHIVE);
    }

    public void testPlatformDefault() {
        ElasticsearchDistribution distro = checkDistro(createProject(null),
            "testdistro", "5.0.0", Type.ARCHIVE, null, Flavor.OSS, true);
        assertEquals(distro.getPlatform(), ElasticsearchDistribution.CURRENT_PLATFORM);
    }

    public void testPlatformForIntegTest() {
        assertDistroError(createProject(null), "testdistro", "5.0.0", Type.INTEG_TEST_ZIP, Platform.LINUX, null, null,
            "platform not allowed for elasticsearch distribution [testdistro]");
    }

    public void testFlavorDefault() {
        ElasticsearchDistribution distro = checkDistro(createProject(null),
            "testdistro", "5.0.0", Type.ARCHIVE, Platform.LINUX, null, true);
        assertEquals(distro.getFlavor(), Flavor.DEFAULT);
    }

    public void testFlavorForIntegTest() {
        assertDistroError(createProject(null),
            "testdistro", "5.0.0", Type.INTEG_TEST_ZIP, null, Flavor.OSS, null,
            "flavor not allowed for elasticsearch distribution [testdistro]");
    }

    public void testBundledJdkDefault() {
        ElasticsearchDistribution distro = checkDistro(createProject(null),
            "testdistro", "5.0.0", Type.ARCHIVE, Platform.LINUX, null, true);
        assertTrue(distro.getBundledJdk());
    }

    public void testBundledJdkForIntegTest() {
        assertDistroError(createProject(null), "testdistro", "5.0.0", Type.INTEG_TEST_ZIP, null, null, true,
            "bundledJdk not allowed for elasticsearch distribution [testdistro]");
    }

    public void testCurrentVersionIntegTestZip() {
        Project project = createProject(null);
        Project archiveProject = ProjectBuilder.builder().withParent(archivesProject).withName("integ-test-zip").build();
        archiveProject.getConfigurations().create("default");
        archiveProject.getArtifacts().add("default", new File("doesnotmatter"));
        createDistro(project, "distro",
            VersionProperties.getElasticsearch(), Type.INTEG_TEST_ZIP, null, null, null);
        checkPlugin(project);
    }

    public void testCurrentVersionArchives() {
        for (Platform platform : Platform.values()) {
            for (Flavor flavor : Flavor.values()) {
                for (boolean bundledJdk : new boolean[] { true, false}) {
                    // create a new project in each iteration, so that we know we are resolving the only additional project being created
                    Project project = createProject(null);
                    String projectName = projectName(platform.toString(), flavor, bundledJdk);
                    projectName += (platform == Platform.WINDOWS ? "-zip" : "-tar");
                    Project archiveProject = ProjectBuilder.builder().withParent(archivesProject).withName(projectName).build();
                    archiveProject.getConfigurations().create("default");
                    archiveProject.getArtifacts().add("default", new File("doesnotmatter"));
                    createDistro(project, "distro",
                        VersionProperties.getElasticsearch(), Type.ARCHIVE, platform, flavor, bundledJdk);
                    checkPlugin(project);
                }
            }
        }
    }

    public void testCurrentVersionPackages() {
        for (Type packageType : new Type[] { Type.RPM, Type.DEB }) {
            for (Flavor flavor : Flavor.values()) {
                for (boolean bundledJdk : new boolean[] { true, false}) {
                    Project project = createProject(null);
                    String projectName = projectName(packageType.toString(), flavor, bundledJdk);
                    Project packageProject = ProjectBuilder.builder().withParent(packagesProject).withName(projectName).build();
                    packageProject.getConfigurations().create("default");
                    packageProject.getArtifacts().add("default", new File("doesnotmatter"));
                    createDistro(project, "distro",
                        VersionProperties.getElasticsearch(), packageType, null, flavor, bundledJdk);
                    checkPlugin(project);
                }
            }
        }
    }

    public void testLocalBwcArchives() {
        for (Platform platform : Platform.values()) {
            for (Flavor flavor : Flavor.values()) {
                // note: no non bundled jdk for bwc
                String configName = projectName(platform.toString(), flavor, true);
                configName += (platform == Platform.WINDOWS ? "-zip" : "-tar");

                checkBwc("minor", configName, BWC_MINOR_VERSION, BWC_MINOR, Type.ARCHIVE, platform, flavor);
                checkBwc("staged", configName, BWC_STAGED_VERSION, BWC_STAGED, Type.ARCHIVE, platform, flavor);
                checkBwc("bugfix", configName, BWC_BUGFIX_VERSION, BWC_BUGFIX, Type.ARCHIVE, platform, flavor);
                checkBwc("maintenance", configName, BWC_MAINTENANCE_VERSION, BWC_MAINTENANCE, Type.ARCHIVE, platform, flavor);
            }
        }
    }

    public void testLocalBwcPackages() {
        for (Type packageType : new Type[] { Type.RPM, Type.DEB }) {
            for (Flavor flavor : Flavor.values()) {
                // note: no non bundled jdk for bwc
                String configName = projectName(packageType.toString(), flavor, true);

                checkBwc("minor", configName, BWC_MINOR_VERSION, BWC_MINOR, packageType, null, flavor);
                checkBwc("staged", configName, BWC_STAGED_VERSION, BWC_STAGED, packageType, null, flavor);
                checkBwc("bugfix", configName, BWC_BUGFIX_VERSION, BWC_BUGFIX, packageType, null, flavor);
                checkBwc("maintenance", configName, BWC_MAINTENANCE_VERSION, BWC_MAINTENANCE, packageType, null, flavor);
            }
        }
    }

    private void assertDistroError(Project project, String name, String version, Type type, Platform platform,
                                   Flavor flavor, Boolean bundledJdk, String message) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> checkDistro(project, name, version, type, platform, flavor, bundledJdk));
        assertThat(e.getMessage(), containsString(message));
    }

    private ElasticsearchDistribution createDistro(Project project, String name, String version, Type type,
                              Platform platform, Flavor flavor, Boolean bundledJdk) {
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
            if (flavor != null) {
                distro.setFlavor(flavor);
            }
            if (bundledJdk != null) {
                distro.setBundledJdk(bundledJdk);
            }
        });
    }

    // create a distro and finalize its configuration
    private ElasticsearchDistribution checkDistro(Project project, String name, String version, Type type,
                                                  Platform platform, Flavor flavor, Boolean bundledJdk) {
        ElasticsearchDistribution distribution = createDistro(project, name, version, type, platform, flavor, bundledJdk);
        distribution.finalizeValues();
        return distribution;
    }

    // check the download plugin can be fully configured
    private void checkPlugin(Project project) {
        DistributionDownloadPlugin plugin = project.getPlugins().getPlugin(DistributionDownloadPlugin.class);
        plugin.setupDistributions(project);
    }

    private void checkBwc(String projectName, String config, Version version, BwcVersions bwcVersions,
                          Type type, Platform platform, Flavor flavor) {
        Project project = createProject(bwcVersions);
        Project archiveProject = ProjectBuilder.builder().withParent(bwcProject).withName(projectName).build();
        archiveProject.getConfigurations().create(config);
        archiveProject.getArtifacts().add(config, new File("doesnotmatter"));
        createDistro(project, "distro", version.toString(), type, platform, flavor, true);
        checkPlugin(project);
    }

    private Project createProject(BwcVersions bwcVersions) {
        rootProject = ProjectBuilder.builder().build();
        Project distributionProject = ProjectBuilder.builder().withParent(rootProject).withName("distribution").build();
        archivesProject = ProjectBuilder.builder().withParent(distributionProject).withName("archives").build();
        packagesProject = ProjectBuilder.builder().withParent(distributionProject).withName("packages").build();
        bwcProject = ProjectBuilder.builder().withParent(distributionProject).withName("bwc").build();
        Project project = ProjectBuilder.builder().withParent(rootProject).build();
        project.getExtensions().getExtraProperties().set("bwcVersions", bwcVersions);
        project.getPlugins().apply("elasticsearch.distribution-download");
        return project;
    }

    private static String projectName(String base, Flavor flavor, boolean bundledJdk) {
        String prefix = "";
        if (flavor == Flavor.OSS) {
            prefix += "oss-";
        }
        if (bundledJdk == false) {
            prefix += "no-jdk-";
        }

        return prefix + base;
    }
}
