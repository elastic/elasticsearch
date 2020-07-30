/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.BwcVersions;
import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.elasticsearch.gradle.DistributionResolution;
import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

import static org.elasticsearch.gradle.util.GradleUtils.projectDependency;

/**
 * An internal elasticsearch build plugin that registers additional
 * distribution resolution strategies to the 'elasticsearch.download-distribution' plugin
 * to resolve distributions from a local snapshot or a locally built bwc snapshot.
 */
public class InternalDistributionDownloadPlugin implements Plugin<Project> {

    private BwcVersions bwcVersions = null;

    @Override
    public void apply(Project project) {
        // this is needed for isInternal
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        if (!BuildParams.isInternal()) {
            throw new GradleException(
                "Plugin 'elasticsearch.internal-distribution-download' is not supported. "
                    + "Use 'elasticsearch.distribution-download' plugin instead."
            );
        }
        project.getPluginManager().apply(DistributionDownloadPlugin.class);
        this.bwcVersions = BuildParams.getBwcVersions();
        registerInternalDistributionResolutions(DistributionDownloadPlugin.getRegistrationsContainer(project));
    }

    /**
     * Registers internal distribution resolutions.
     *
     * Elasticsearch distributions are resolved as project dependencies either representing
     * the current version pointing to a project either under `:distribution:archives` or :distribution:packages`.
     *
     * BWC versions are resolved as project to projects under `:distribution:bwc`.
     * */
    private void registerInternalDistributionResolutions(NamedDomainObjectContainer<DistributionResolution> resolutions) {

        resolutions.register("localBuild", distributionResolution -> distributionResolution.setResolver((project, distribution) -> {
            if (VersionProperties.getElasticsearch().equals(distribution.getVersion())) {
                // non-external project, so depend on local build
                return projectDependency(project, distributionProjectPath(distribution), "default");
            }
            return null;
        }));

        resolutions.register("bwc", distributionResolution -> distributionResolution.setResolver((project, distribution) -> {
            BwcVersions.UnreleasedVersionInfo unreleasedInfo = bwcVersions.unreleasedInfo(Version.fromString(distribution.getVersion()));
            if (unreleasedInfo != null) {
                if (!distribution.getBundledJdk()) {
                    throw new GradleException(
                        "Configuring a snapshot bwc distribution ('"
                            + distribution.getName()
                            + "') "
                            + "without a bundled JDK is not supported."
                    );
                }
                return projectDependency(project, unreleasedInfo.gradleProjectPath, distributionProjectName(distribution));
            }
            return null;
        }));
    }

    private static String distributionProjectPath(ElasticsearchDistribution distribution) {
        String projectPath = ":distribution";
        switch (distribution.getType()) {
            case INTEG_TEST_ZIP:
                projectPath += ":archives:integ-test-zip";
                break;

            case DOCKER:
                projectPath += ":docker:";
                projectPath += distributionProjectName(distribution);
                break;

            default:
                projectPath += distribution.getType() == ElasticsearchDistribution.Type.ARCHIVE ? ":archives:" : ":packages:";
                projectPath += distributionProjectName(distribution);
                break;
        }
        return projectPath;
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

        if (distribution.getFlavor() == ElasticsearchDistribution.Flavor.OSS) {
            projectName += "oss-";
        }

        if (distribution.getBundledJdk() == false) {
            projectName += "no-jdk-";
        }

        switch (distribution.getType()) {
            case ARCHIVE:
                projectName += platform.toString() + archString + (platform == ElasticsearchDistribution.Platform.WINDOWS
                    ? "-zip"
                    : "-tar");
                break;

            case DOCKER:
                projectName += "docker" + archString + "-export";
                break;

            default:
                projectName += distribution.getType();
                break;
        }
        return projectName;
    }
}
