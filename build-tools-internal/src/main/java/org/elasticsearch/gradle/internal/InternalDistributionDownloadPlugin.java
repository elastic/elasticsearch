/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.DistributionDependency;
import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.elasticsearch.gradle.DistributionResolution;
import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.distribution.ElasticsearchDistributionTypes;
import org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes;
import org.elasticsearch.gradle.internal.docker.DockerSupportPlugin;
import org.elasticsearch.gradle.internal.docker.DockerSupportService;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.provider.Provider;

import java.util.function.Function;

import static org.elasticsearch.gradle.util.GradleUtils.projectDependency;

/**
 * An internal elasticsearch build plugin that registers additional
 * distribution resolution strategies to the 'elasticsearch.download-distribution' plugin
 * to resolve distributions from a local snapshot or a locally built bwc snapshot.
 */
public class InternalDistributionDownloadPlugin implements InternalPlugin {

    @Override
    public void apply(Project project) {
        // this is needed for isInternal
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        project.getRootProject().getPluginManager().apply(DockerSupportPlugin.class);
        DistributionDownloadPlugin distributionDownloadPlugin = project.getPlugins().apply(DistributionDownloadPlugin.class);
        Provider<DockerSupportService> dockerSupport = GradleUtils.getBuildService(
            project.getGradle().getSharedServices(),
            DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME
        );
        distributionDownloadPlugin.setDockerAvailability(
            dockerSupport.map(dockerSupportService -> dockerSupportService.getDockerAvailability().isAvailable)
        );
        registerInternalDistributionResolutions(DistributionDownloadPlugin.getRegistrationsContainer(project));
    }

    /**
     * Registers internal distribution resolutions.
     * <p>
     * Elasticsearch distributions are resolved as project dependencies either representing
     * the current version pointing to a project either under `:distribution:archives` or :distribution:packages`.
     * <p>
     * BWC versions are resolved as project to projects under `:distribution:bwc`.
     */
    private void registerInternalDistributionResolutions(NamedDomainObjectContainer<DistributionResolution> resolutions) {
        resolutions.register("localBuild", distributionResolution -> distributionResolution.setResolver((project, distribution) -> {
            if (VersionProperties.getElasticsearch().equals(distribution.getVersion())) {
                // non-external project, so depend on local build
                return new ProjectBasedDistributionDependency(
                    config -> projectDependency(project, distributionProjectPath(distribution), config)
                );
            }
            return null;
        }));

        resolutions.register("bwc", distributionResolution -> distributionResolution.setResolver((project, distribution) -> {
            BwcVersions.UnreleasedVersionInfo unreleasedInfo = BuildParams.getBwcVersions().unreleasedInfo(Version.fromString(distribution.getVersion()));
            if (unreleasedInfo != null) {
                if (distribution.getBundledJdk() == false) {
                    throw new GradleException(
                        "Configuring a snapshot bwc distribution ('"
                            + distribution.getName()
                            + "') "
                            + "without a bundled JDK is not supported."
                    );
                }
                String projectConfig = getProjectConfig(distribution, unreleasedInfo);
                return new ProjectBasedDistributionDependency(
                    (config) -> projectDependency(project, unreleasedInfo.gradleProjectPath, projectConfig)
                );
            }
            return null;
        }));
    }

    /**
     * Will be removed once this is backported to all unreleased branches.
     */
    private static String getProjectConfig(ElasticsearchDistribution distribution, BwcVersions.UnreleasedVersionInfo info) {
        String distributionProjectName = distributionProjectName(distribution);
        if (distribution.getType().shouldExtract()) {
            return (info.gradleProjectPath.equals(":distribution") || info.version.before("7.10.0"))
                ? distributionProjectName
                : "expanded-" + distributionProjectName;
        } else {
            return distributionProjectName;
        }
    }

    private static String distributionProjectPath(ElasticsearchDistribution distribution) {
        String projectPath = ":distribution";
        if (distribution.getType() == ElasticsearchDistributionTypes.INTEG_TEST_ZIP) {
            projectPath += ":archives:integ-test-zip";
        } else if (distribution.getType().isDocker()) {
            projectPath += ":docker:";
            projectPath += distributionProjectName(distribution);
        } else {
            projectPath += distribution.getType() == ElasticsearchDistributionTypes.ARCHIVE ? ":archives:" : ":packages:";
            projectPath += distributionProjectName(distribution);
        }
        return projectPath;
    }

    @Override
    public String getExternalUseErrorMessage() {
        return "Plugin 'elasticsearch.internal-distribution-download' is not supported. "
            + "Use 'elasticsearch.distribution-download' plugin instead.";
    }

    /**
     * Works out the gradle project name that provides a distribution artifact.
     *
     * @param distribution the distribution from which to derive a project name
     * @return the name of a project. It is not the full project path, only the name.
     */
    private static String distributionProjectName(ElasticsearchDistribution distribution) {
        ElasticsearchDistribution.Platform platform = distribution.getPlatform();
        Architecture architecture = distribution.getArchitecture();
        String projectName = "";

        final String archString = platform == ElasticsearchDistribution.Platform.WINDOWS || architecture == Architecture.X64
            ? ""
            : "-" + architecture.toString().toLowerCase();

        if (distribution.getBundledJdk() == false) {
            projectName += "no-jdk-";
        }

        if (distribution.getType() == ElasticsearchDistributionTypes.ARCHIVE) {
            return projectName + platform.toString() + archString + (platform == ElasticsearchDistribution.Platform.WINDOWS
                ? "-zip"
                : "-tar");
        }
        if (distribution.getType() == InternalElasticsearchDistributionTypes.DOCKER) {
            return projectName + "docker" + archString + "-export";
        }
        if (distribution.getType() == InternalElasticsearchDistributionTypes.DOCKER_UBI) {
            return projectName + "ubi-docker" + archString + "-export";
        }
        if (distribution.getType() == InternalElasticsearchDistributionTypes.DOCKER_IRONBANK) {
            return projectName + "ironbank-docker" + archString + "-export";
        }
        if (distribution.getType() == InternalElasticsearchDistributionTypes.DOCKER_CLOUD) {
            return projectName + "cloud-docker" + archString + "-export";
        }
        if (distribution.getType() == InternalElasticsearchDistributionTypes.DOCKER_CLOUD_ESS) {
            return projectName + "cloud-ess-docker" + archString + "-export";
        }
        return projectName + distribution.getType().getName();
    }

    private static class ProjectBasedDistributionDependency implements DistributionDependency {

        private Function<String, Dependency> function;

        ProjectBasedDistributionDependency(Function<String, Dependency> function) {
            this.function = function;
        }

        @Override
        public Object getDefaultNotation() {
            return function.apply("default");
        }

        @Override
        public Object getExtractedNotation() {
            return function.apply("extracted");
        }
    }
}
