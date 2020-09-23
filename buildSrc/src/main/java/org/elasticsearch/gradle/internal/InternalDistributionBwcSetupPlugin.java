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

import org.elasticsearch.gradle.BwcVersions;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;

/**
 * We want to be able to do BWC tests for unreleased versions without relying on and waiting for snapshots.
 * For this we need to check out and build the unreleased versions.
 * Since These depend on the current version, we can't name the Gradle projects statically, and don't know what the
 * unreleased versions are when Gradle projects are set up, so we use "build-unreleased-version-*" as placeholders
 * and configure them to build various versions here.
 */
public class InternalDistributionBwcSetupPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        Provider<String> remote = project.getProviders().systemProperty("bwc.remote").forUseAtConfigurationTime().orElse("elastic");

        BuildParams.getBwcVersions()
            .forPreviousUnreleased(
                (BwcVersions.UnreleasedVersionInfo unreleasedVersion) -> {
                    configureProject(project.project(unreleasedVersion.gradleProjectPath), unreleasedVersion, remote);
                }
            );
    }

    private void configureProject(Project bwcProject, BwcVersions.UnreleasedVersionInfo unreleasedVersion, Provider<String> remote) {
        BwcGitExtension gitExtension = bwcProject.getPlugins().apply(InternalBwcGitPlugin.class).getGitExtension();
        String bwcBranch = unreleasedVersion.branch;
        Version bwcVersion = unreleasedVersion.version;
        File checkoutDir = new File(bwcProject.getBuildDir(), "bwc/checkout-" + bwcBranch);
        BwcSetupExtension bwcSetupExtension = bwcProject.getExtensions()
            .create("bwcSetup", BwcSetupExtension.class, bwcProject, checkoutDir, bwcVersion);

        gitExtension.getBwcVersion().set(unreleasedVersion.version);
        gitExtension.getBwcBranch().set(bwcBranch);
        gitExtension.setCheckoutDir(checkoutDir);

        bwcProject.getPlugins().apply("distribution");
        // Not published so no need to assemble
        bwcProject.getTasks().named("assemble").configure(t -> t.setEnabled(false));

        TaskProvider<Task> buildBwcTaskProvider = bwcProject.getTasks().register("buildBwc");
        List<ArchiveProject> archiveProjects = resolveArchiveProjects(checkoutDir, bwcVersion);

        for (ArchiveProject archiveProject : archiveProjects) {
            createBuildBwcTask(
                bwcSetupExtension,
                bwcProject,
                bwcVersion,
                archiveProject.name,
                archiveProject.getProjectPath(),
                archiveProject.getDistArchiveFile(),
                buildBwcTaskProvider
            );

            registerBwcArtifacts(bwcProject, archiveProject);
        }

        // Create build tasks for the JDBC driver used for compatibility testing
        String jdbcProjectDir = "x-pack/plugin/sql/jdbc";

        File jdbcProjectArtifact = new File(
            checkoutDir,
            jdbcProjectDir + "/build/distributions/x-pack-sql-jdbc-" + bwcVersion + "-SNAPSHOT.jar"
        );

        createBuildBwcTask(bwcSetupExtension, bwcProject, bwcVersion, "jdbc", jdbcProjectDir, jdbcProjectArtifact, buildBwcTaskProvider);

        // make sure no dependencies were added to assemble; we want it to be a no-op
        bwcProject.getTasks().named("assemble").configure(t -> t.setDependsOn(Collections.emptyList()));
    }

    private void registerBwcArtifacts(Project bwcProject, ArchiveProject archiveProject) {
        String projectName = archiveProject.name;
        String buildBwcTask = buildBwcTaskName(projectName);

        registerDistributionArchiveArtifact(bwcProject, archiveProject, buildBwcTask);
        if (archiveProject.getExplodedDistDirectory() != null) {
            String explodedDistConfiguration = "exploded-" + projectName;
            bwcProject.getConfigurations().create(explodedDistConfiguration);
            bwcProject.getArtifacts().add(explodedDistConfiguration, archiveProject.getExplodedDistDirectory(), artifact -> {
                artifact.setName("elasticsearch");
                artifact.builtBy(buildBwcTask);
                artifact.setType("directory");
            });
        }
    }

    private void registerDistributionArchiveArtifact(Project bwcProject, ArchiveProject archiveProject, String buildBwcTask) {
        String artifactFileName = archiveProject.getDistArchiveFile().getName();
        String artifactName = artifactFileName.contains("oss") ? "elasticsearch-oss" : "elasticsearch";

        String suffix = artifactFileName.endsWith("tar.gz") ? "tar.gz" : artifactFileName.substring(artifactFileName.length() - 3);
        int archIndex = artifactFileName.indexOf("x86_64");

        bwcProject.getConfigurations().create(archiveProject.name);
        bwcProject.getArtifacts().add(archiveProject.name, archiveProject.getDistArchiveFile(), artifact -> {
            artifact.setName(artifactName);
            artifact.builtBy(buildBwcTask);
            artifact.setType(suffix);

            String classifier = "";
            if (archIndex != -1) {
                int osIndex = artifactFileName.lastIndexOf('-', archIndex - 2);
                classifier = "-" + artifactFileName.substring(osIndex + 1, archIndex - 1) + "-x86_64";
            }
            artifact.setClassifier(classifier);
        });
    }

    private static List<ArchiveProject> resolveArchiveProjects(File checkoutDir, Version bwcVersion) {
        List<String> projects = new ArrayList<>();
        projects.addAll(asList("deb", "rpm"));
        if (bwcVersion.onOrAfter("7.0.0")) {
            projects.addAll(asList("oss-windows-zip", "windows-zip", "oss-darwin-tar", "darwin-tar", "oss-linux-tar", "linux-tar"));
        } else if (bwcVersion.onOrAfter("6.3.0")) {
            projects.addAll(asList("oss-zip", "zip", "oss-deb", "oss-rpm"));
        } else {
            projects.addAll(asList("zip"));
        }

        return projects.stream().map(name -> {
            String baseDir = "distribution";
            if (bwcVersion.onOrAfter("6.3.0")) {
                baseDir = baseDir + (name.endsWith("zip") || name.endsWith("tar") ? "/archives" : "/packages");
            }
            String classifier = "";
            String extension = name;
            if (bwcVersion.onOrAfter("7.0.0") && (name.contains("zip") || name.contains("tar"))) {
                int index = name.lastIndexOf('-');
                String baseName = name.startsWith("oss-") ? name.substring(4, index) : name.substring(0, index);
                classifier = "-" + baseName + "-x86_64";
                extension = name.substring(index + 1);
                if (extension.equals("tar")) {
                    extension += ".gz";
                }
            } else if (bwcVersion.onOrAfter("7.0.0") && name.contains("deb")) {
                classifier = "-amd64";
            } else if (bwcVersion.onOrAfter("7.0.0") && name.contains("rpm")) {
                classifier = "-x86_64";
            }
            return new ArchiveProject(name, baseDir, bwcVersion, classifier, extension, checkoutDir);
        }).collect(Collectors.toList());
    }

    private String buildBwcTaskName(String projectName) {
        return "buildBwc"
            + stream(projectName.split("-")).map(i -> i.substring(0, 1).toUpperCase(Locale.ROOT) + i.substring(1))
                .collect(Collectors.joining());
    }

    void createBuildBwcTask(
        BwcSetupExtension bwcSetupExtension,
        Project project,
        Version bwcVersion,
        String projectName,
        String projectPath,
        File projectArtifact,
        TaskProvider<Task> bwcTaskProvider
    ) {
        String bwcTaskName = buildBwcTaskName(projectName);
        bwcSetupExtension.bwcTask(bwcTaskName, c -> {
            c.getInputs().file(new File(project.getBuildDir(), "refspec"));
            c.getOutputs().files(projectArtifact);
            c.getOutputs().cacheIf("BWC distribution caching is disabled on 'master' branch", task -> {
                String gitBranch = System.getenv("GIT_BRANCH");
                return BuildParams.isCi() && (gitBranch == null || gitBranch.endsWith("master") == false);
            });
            c.args(projectPath.replace('/', ':') + ":assemble");
            if (project.getGradle().getStartParameter().isBuildCacheEnabled()) {
                c.args("--build-cache");
            }
            c.doLast(task -> {
                if (projectArtifact.exists() == false) {
                    throw new InvalidUserDataException("Building " + bwcVersion + " didn't generate expected file " + projectArtifact);
                }
            });
        });
        bwcTaskProvider.configure(t -> t.dependsOn(bwcTaskName));
    }

    /**
     * Represents an archive project (distribution/archives/*)
     * we build from a bwc Version in a cloned repository
     */
    private static class ArchiveProject {
        private final String name;
        private String projectPath;
        private File distArchiveFile;
        private File explodedDistDir;

        ArchiveProject(String name, String baseDir, Version version, String classifier, String extension, File checkoutDir) {
            this.name = name;
            this.projectPath = baseDir + "/" + name;
            this.distArchiveFile = new File(
                checkoutDir,
                baseDir
                    + "/"
                    + name
                    + "/build/distributions/elasticsearch-"
                    + (name.startsWith("oss") ? "oss-" : "")
                    + version
                    + "-SNAPSHOT"
                    + classifier
                    + "."
                    + extension
            );
            // we only ported this down to the 7.x branch.
            if (version.onOrAfter("7.10.0") && (name.endsWith("zip") || name.endsWith("tar"))) {
                this.explodedDistDir = new File(checkoutDir, baseDir + "/" + name + "/build/install");
            }
        }

        public String getProjectPath() {
            return projectPath;
        }

        public File getDistArchiveFile() {
            return distArchiveFile;
        }

        public File getExplodedDistDirectory() {
            return explodedDistDir;
        }
    }
}
