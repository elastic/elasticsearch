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

import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.UnknownTaskException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.gradle.api.credentials.HttpHeaderCredentials;
import org.gradle.api.file.FileTree;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.authentication.http.HttpHeaderAuthentication;

import java.io.File;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * A plugin to manage getting and extracting distributions of Elasticsearch.
 *
 * The source of the distribution could be from a local snapshot, a locally built
 * bwc snapshot, or the Elastic downloads service.
 */
public class DistributionDownloadPlugin implements Plugin<Project> {

    private static final String FAKE_GROUP = "elasticsearch-distribution";
    private static final String DOWNLOAD_REPO_NAME = "elasticsearch-downloads";

    private BwcVersions bwcVersions;
    private NamedDomainObjectContainer<ElasticsearchDistribution> distributionsContainer;

    @Override
    public void apply(Project project) {
        distributionsContainer = project.container(ElasticsearchDistribution.class, name -> new ElasticsearchDistribution(name, project));
        project.getExtensions().add("elasticsearch_distributions", distributionsContainer);

        setupDownloadServiceRepo(project);

        ExtraPropertiesExtension extraProperties = project.getExtensions().getExtraProperties();
        if (extraProperties.has("bwcVersions")) {
            this.bwcVersions = (BwcVersions) extraProperties.get("bwcVersions");
        } // else - leniency for external plugins...TODO: setup snapshot dependency instead

        project.afterEvaluate(this::setupDistributions);
    }

    // pkg private for tests
    void setupDistributions(Project project) {
        for (ElasticsearchDistribution distribution : distributionsContainer) {
            distribution.finalizeValues();

            DependencyHandler dependencies = project.getDependencies();
            // for the distribution as a file, just depend on the artifact directly
            dependencies.add(distribution.configuration.getName(), dependencyNotation(project, distribution));

            // no extraction allowed for rpm or deb
            if (distribution.getType().equals("rpm") == false && distribution.getType().equals("deb") == false) {
                // for the distribution extracted, add a root level task that does the extraction, and depend on that
                // extracted configuration as a fake artifact
                dependencies.add(distribution.getExtracted().configuration.getName(),
                    projectDependency(project, ":", configName("extracted_elasticsearch", distribution)));
                // ensure a root level download task exists
                setupRootDownload(project.getRootProject(), distribution);
            }
        }
    }

    private void setupRootDownload(Project rootProject, ElasticsearchDistribution distribution) {
        String extractTaskName = extractTaskName(distribution);
        // NOTE: this is *horrendous*, but seems to be the only way to check for the existence of a registered task
        try {
            rootProject.getTasks().named(extractTaskName);
            // already setup this version
            return;
        } catch (UnknownTaskException e) {
            // fall through: register the task
        }
        setupDownloadServiceRepo(rootProject);

        final ConfigurationContainer configurations = rootProject.getConfigurations();
        String downloadConfigName = configName("elasticsearch", distribution);
        String extractedConfigName = "extracted_" + downloadConfigName;
        Configuration downloadConfig = configurations.findByName(downloadConfigName);
        if (downloadConfig == null) {
            downloadConfig = configurations.create(downloadConfigName);
            configurations.create(extractedConfigName);
        }
        Object distroDep = dependencyNotation(rootProject, distribution);
        rootProject.getDependencies().add(downloadConfigName, distroDep);

        // add task for extraction, delaying resolving config until runtime
        if (distribution.getType().equals("archive") || distribution.getType().equals("integ-test-zip")) {
            Supplier<File> archiveGetter = downloadConfig::getSingleFile;
            final Callable<FileTree> fileGetter;
            if (distribution.getType().equals("integ-test-zip") || distribution.getPlatform().equals("windows")) {
                fileGetter = () -> rootProject.zipTree(archiveGetter.get());
            } else {
                fileGetter = () -> rootProject.tarTree(rootProject.getResources().gzip(archiveGetter.get()));
            }
            String extractDir = rootProject.getBuildDir().toPath().resolve("elasticsearch-distros").resolve(extractedConfigName).toString();
            TaskProvider<Copy> extractTask = rootProject.getTasks().register(extractTaskName, Copy.class, copyTask -> {
                copyTask.doFirst(t -> rootProject.delete(extractDir));
                copyTask.into(extractDir);
                copyTask.from(fileGetter);
            });
            rootProject.getArtifacts().add(extractedConfigName,
                rootProject.getLayout().getProjectDirectory().dir(extractDir),
                artifact -> artifact.builtBy(extractTask));
        }
    }

    private static void setupDownloadServiceRepo(Project project) {
        if (project.getRepositories().findByName(DOWNLOAD_REPO_NAME) != null) {
            return;
        }
        project.getRepositories().ivy(ivyRepo -> {
            ivyRepo.setName(DOWNLOAD_REPO_NAME);
            ivyRepo.setUrl("https://artifacts.elastic.co");
            ivyRepo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
            // this header is not a credential but we hack the capability to send this header to avoid polluting our download stats
            ivyRepo.credentials(HttpHeaderCredentials.class, creds -> {
                creds.setName("X-Elastic-No-KPI");
                creds.setValue("1");
            });
            ivyRepo.getAuthentication().create("header", HttpHeaderAuthentication.class);
            ivyRepo.patternLayout(layout -> layout.artifact("/downloads/elasticsearch/[module]-[revision](-[classifier]).[ext]"));
            ivyRepo.content(content -> content.includeGroup(FAKE_GROUP));
        });
        project.getRepositories().all(repo -> {
            if (repo.getName().equals(DOWNLOAD_REPO_NAME) == false) {
                // all other repos should ignore the special group name
                repo.content(content -> content.excludeGroup(FAKE_GROUP));
            }
        });
        // TODO: need maven repo just for integ-test-zip, but only in external cases
    }

    private Object dependencyNotation(Project project, ElasticsearchDistribution distribution) {

        if (Version.fromString(VersionProperties.getElasticsearch()).equals(distribution.getVersion())) {
            return projectDependency(project, distributionProjectPath(distribution), "default");
            // TODO: snapshot dep when not in ES repo
        }
        BwcVersions.UnreleasedVersionInfo unreleasedInfo = bwcVersions == null ?
            null : bwcVersions.unreleasedInfo(distribution.getVersion());
        if (unreleasedInfo != null) {
            assert distribution.getBundledJdk();
            return projectDependency(project, unreleasedInfo.gradleProjectPath, distributionProjectName(distribution));
        }

        if (distribution.getType().equals("integ-test-zip")) {
            return "org.elasticsearch.distribution.integ-test-zip:elasticsearch:" + distribution.getVersion();
        }

        String extension = distribution.getType();
        String classifier = "x86_64";
        if (distribution.getType().equals("archive")) {
            extension = distribution.getPlatform().equals("windows") ? "zip" : "tar.gz";
            classifier = distribution.getPlatform() + "-" + classifier;
        }
        return FAKE_GROUP + ":elasticsearch" + (distribution.getType().equals("oss") ? "-oss:" : ":")
            + distribution.getVersion() + ":" + classifier + "@" + extension;
    }

    private static Dependency projectDependency(Project project, String projectPath, String projectConfig) {
        if (project.findProject(projectPath) == null) {
            throw new GradleException("no project [" + projectPath + "], project names: " + project.getRootProject().getAllprojects());
        }
        Map<String, Object> depConfig = new HashMap<>();
        depConfig.put("path", projectPath);
        depConfig.put("configuration", projectConfig);
        return project.getDependencies().project(depConfig);
    }

    private static String distributionProjectPath(ElasticsearchDistribution distribution) {
        String projectPath = ":distribution";
        if (distribution.getType().equals("integ-test-zip")) {
            projectPath += ":archives:integ-test-zip";
        } else {
            projectPath += distribution.getType().equals("archive") ? ":archives:" : ":packages:";
            projectPath += distributionProjectName(distribution);
        }
        return projectPath;
    }

    private static String distributionProjectName(ElasticsearchDistribution distribution) {
        String projectName = "";
        if (distribution.getFlavor().equals("oss")) {
            projectName += "oss-";
        }
        if (distribution.getBundledJdk() == false) {
            projectName += "no-jdk-";
        }
        if (distribution.getType().equals("archive")) {
            String platform = distribution.getPlatform();
            projectName += platform + (platform.equals("windows") ? "-zip" : "-tar");
        } else {
            projectName += distribution.getType();
        }
        return projectName;
    }

    private static String configName(String prefix, ElasticsearchDistribution distribution) {
        return prefix + "_" + distribution.getVersion() + "_" + distribution.getPlatform() + "_" + distribution.getType() + "_"
            + distribution.getFlavor() + (distribution.getBundledJdk() ? "" : "_nojdk");
    }

    private static String capitalize(String s) {
        return s.substring(0, 1).toUpperCase(Locale.ROOT) + s.substring(1);
    }

    private static String extractTaskName(ElasticsearchDistribution distribution) {
        String taskName = "extractElasticsearch";
        if (distribution.getType().equals("integ-test-zip") == false) {
            if (distribution.getFlavor().equals("oss")) {
                taskName += "Oss";
            }
            if (distribution.getBundledJdk() == false) {
                taskName += "NoJdk";
            }
        }
        if (distribution.getType().equals("archive")) {
            taskName += capitalize(distribution.getPlatform());
        } else if (distribution.getType().equals("integ-test-zip") == false) {
            taskName += capitalize(distribution.getType());
        }
        taskName += distribution.getVersion();
        return taskName;
    }
}
