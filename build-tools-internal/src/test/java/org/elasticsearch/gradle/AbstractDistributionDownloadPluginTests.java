/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.elasticsearch.gradle.internal.BwcVersions;
import org.elasticsearch.gradle.internal.BwcVersions.VersionPair;
import org.elasticsearch.gradle.internal.test.GradleUnitTestCase;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

import java.io.File;
import java.util.Arrays;

public class AbstractDistributionDownloadPluginTests extends GradleUnitTestCase {
    protected static Project rootProject;
    protected static Project archivesProject;
    protected static Project packagesProject;
    protected static Project bwcProject;

    protected static final VersionPair BWC_MAJOR_VERSION = new VersionPair(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
    protected static final VersionPair BWC_MINOR_VERSION = new VersionPair(Version.fromString("1.1.0"), Version.fromString("2.1.0"));
    protected static final VersionPair BWC_STAGED_VERSION = new VersionPair(Version.fromString("1.0.0"), Version.fromString("2.0.0"));
    protected static final VersionPair BWC_BUGFIX_VERSION = new VersionPair(Version.fromString("1.0.1"), Version.fromString("2.0.0"));
    protected static final VersionPair BWC_MAINTENANCE_VERSION = new VersionPair(Version.fromString("0.90.1"), Version.fromString("1.1.3"));

    protected static final BwcVersions BWC_MINOR = new BwcVersions(
        BWC_MAJOR_VERSION.elasticsearch(),
        Arrays.asList(BWC_BUGFIX_VERSION, BWC_MINOR_VERSION, BWC_MAJOR_VERSION)
    );
    protected static final BwcVersions BWC_STAGED = new BwcVersions(
        BWC_MAJOR_VERSION.elasticsearch(),
        Arrays.asList(BWC_MAINTENANCE_VERSION, BWC_STAGED_VERSION, BWC_MINOR_VERSION, BWC_MAJOR_VERSION)
    );
    protected static final BwcVersions BWC_BUGFIX = new BwcVersions(
        BWC_MAJOR_VERSION.elasticsearch(),
        Arrays.asList(BWC_BUGFIX_VERSION, BWC_MINOR_VERSION, BWC_MAJOR_VERSION)
    );
    protected static final BwcVersions BWC_MAINTENANCE = new BwcVersions(
        BWC_MINOR_VERSION.elasticsearch(),
        Arrays.asList(BWC_MAINTENANCE_VERSION, BWC_BUGFIX_VERSION, BWC_MINOR_VERSION)
    );

    protected static String projectName(String base, boolean bundledJdk) {
        String prefix = bundledJdk == false ? "no-jdk-" : "";
        return prefix + base;
    }

    protected void checkBwc(
        String projectName,
        String config,
        Version version,
        ElasticsearchDistributionType type,
        ElasticsearchDistribution.Platform platform,
        BwcVersions bwcVersions
    ) {
        Project project = createProject(bwcVersions);
        Project archiveProject = ProjectBuilder.builder().withParent(bwcProject).withName(projectName).build();
        archiveProject.getConfigurations().create(config);
        archiveProject.getArtifacts().add(config, new File("doesnotmatter"));
        createDistro(project, "distro", version.toString(), type, platform, true);
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

    protected Project createProject(BwcVersions bwcVersions) {
        rootProject = ProjectBuilder.builder().build();
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

}
