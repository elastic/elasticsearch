/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle;

import org.elasticsearch.gradle.internal.BwcVersions;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class AbstractDistributionDownloadPluginTests {
    protected static Project rootProject;
    protected static Project archivesProject;
    protected static Project packagesProject;
    protected static Project bwcProject;

    protected static final Version BWC_MAJOR_VERSION = Version.fromString("2.0.0");
    protected static final Version BWC_MINOR_VERSION = Version.fromString("1.1.0");
    protected static final Version BWC_STAGED_VERSION = Version.fromString("1.0.0");
    protected static final Version BWC_BUGFIX_VERSION = Version.fromString("1.0.1");
    protected static final Version BWC_MAINTENANCE_VERSION = Version.fromString("0.90.1");
    protected static final List<String> DEVELOPMENT_BRANCHES = Arrays.asList("main", "1.1", "1.0", "0.90");

    protected static final BwcVersions BWC_MINOR = new BwcVersions(
        BWC_MAJOR_VERSION,
        Arrays.asList(BWC_BUGFIX_VERSION, BWC_MINOR_VERSION, BWC_MAJOR_VERSION),
        DEVELOPMENT_BRANCHES
    );
    protected static final BwcVersions BWC_STAGED = new BwcVersions(
        BWC_MAJOR_VERSION,
        Arrays.asList(BWC_MAINTENANCE_VERSION, BWC_STAGED_VERSION, BWC_MINOR_VERSION, BWC_MAJOR_VERSION),
        DEVELOPMENT_BRANCHES
    );
    protected static final BwcVersions BWC_BUGFIX = new BwcVersions(
        BWC_MAJOR_VERSION,
        Arrays.asList(BWC_BUGFIX_VERSION, BWC_MINOR_VERSION, BWC_MAJOR_VERSION),
        DEVELOPMENT_BRANCHES
    );
    protected static final BwcVersions BWC_MAINTENANCE = new BwcVersions(
        BWC_MINOR_VERSION,
        Arrays.asList(BWC_MAINTENANCE_VERSION, BWC_BUGFIX_VERSION, BWC_MINOR_VERSION),
        DEVELOPMENT_BRANCHES
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
