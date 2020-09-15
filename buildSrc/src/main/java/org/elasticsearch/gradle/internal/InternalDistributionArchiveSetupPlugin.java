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

import org.elasticsearch.gradle.EmptyDirTask;
import org.elasticsearch.gradle.tar.SymbolicLinkPreservingTar;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.tasks.AbstractCopyTask;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;
import org.gradle.api.tasks.bundling.Compression;
import org.gradle.api.tasks.bundling.Zip;

import java.io.File;

import static org.elasticsearch.gradle.util.Util.capitalize;
import static org.gradle.api.internal.artifacts.ArtifactAttributes.ARTIFACT_FORMAT;

/**
 * Provides a DSL and common configurations to define different types of
 * Elasticsearch distribution archives. See ':distribution:archives'.
 * <p>
 * This configures the default artifacts for the distribution specific
 * subprojects. We have subprojects for two reasons:
 * 1. Gradle project substitutions can only bind to the default
 * configuration of a project
 * 2. The integ-test-zip and zip distributions have the exact same
 * filename, so they must be placed in different directories.
 * 3. We provide a packed and an unpacked variant of the distribution
 * - the unpacked variant is used by consumers like test cluster definitions
 * 4. Having per-distribution sub-projects means we can build them in parallel.
 */
public class InternalDistributionArchiveSetupPlugin implements Plugin<Project> {

    public static final String DEFAULT_CONFIGURATION_NAME = "default";
    public static final String EXTRACTED_CONFIGURATION_NAME = "extracted";
    private NamedDomainObjectContainer<DistributionArchive> container;

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(BasePlugin.class);
        registerAndConfigureDistributionArchivesExtension(project);
        registerEmptyDirectoryTasks(project);
        configureGeneralTaskDefaults(project);
        configureTarDefaults(project);
    }

    private void registerAndConfigureDistributionArchivesExtension(Project project) {
        container = project.container(DistributionArchive.class, name -> {
            var subProjectDir = archiveToSubprojectName(name);
            var copyDistributionTaskName = "build" + capitalize(name.substring(0, name.length() - 3));
            TaskContainer tasks = project.getTasks();
            var explodedDist = tasks.register(copyDistributionTaskName, Copy.class, copy -> copy.into(subProjectDir + "/build/install/"));
            var archiveTaskName = "build" + capitalize(name);
            return name.endsWith("Tar")
                ? new DistributionArchive(tasks.register(archiveTaskName, SymbolicLinkPreservingTar.class), explodedDist, name)
                : new DistributionArchive(tasks.register(archiveTaskName, Zip.class), explodedDist, name);
        });
        // Each defined distribution archive is linked to a subproject.
        // A distribution archive definition not matching a sub project will result in build failure.
        container.whenObjectAdded(distributionArchive -> {
            var subProjectName = archiveToSubprojectName(distributionArchive.getName());
            project.project(subProjectName, sub -> {
                sub.getPlugins().apply(BasePlugin.class);
                sub.getArtifacts().add(DEFAULT_CONFIGURATION_NAME, distributionArchive.getArchiveTask());
                var extractedConfiguration = sub.getConfigurations().create("extracted");
                extractedConfiguration.setCanBeResolved(false);
                extractedConfiguration.getAttributes().attribute(ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
                sub.getArtifacts().add(EXTRACTED_CONFIGURATION_NAME, distributionArchive.getExplodedArchiveTask());

            });
        });
        project.getExtensions().add("distribution_archives", container);
    }

    private void configureGeneralTaskDefaults(Project project) {
        // common config across all copy / archive tasks
        project.getTasks().withType(AbstractCopyTask.class).configureEach(t -> {
            t.dependsOn(project.getTasks().withType(EmptyDirTask.class));
            t.setIncludeEmptyDirs(true);
            t.setDirMode(0755);
            t.setFileMode(0644);
        });

        // common config across all archives
        project.getTasks().withType(AbstractArchiveTask.class).configureEach(t -> {
            String subdir = archiveTaskToSubprojectName(t.getName());
            t.getDestinationDirectory().set(project.file(subdir + "/build/distributions"));
            t.getArchiveBaseName().set(subdir.contains("oss") ? "elasticsearch-oss" : "elasticsearch");
        });
    }

    private void configureTarDefaults(Project project) {
        // common config across all tars
        project.getTasks().withType(SymbolicLinkPreservingTar.class).configureEach(t -> {
            t.getArchiveExtension().set("tar.gz");
            t.setCompression(Compression.GZIP);
        });
    }

    private void registerEmptyDirectoryTasks(Project project) {
        // CopySpec does not make it easy to create an empty directory so we
        // create the directory that we want, and then point CopySpec to its
        // parent to copy to the root of the distribution
        File logsDir = new File(project.getBuildDir(), "logs-hack/logs");
        project.getExtensions().getExtraProperties().set("logsDir", new File(project.getBuildDir(), "logs-hack/logs"));
        project.getTasks().register("createLogsDir", EmptyDirTask.class, t -> {
            t.setDir(logsDir);
            t.setDirMode(0755);
        });

        File pluginsDir = new File(project.getBuildDir(), "plugins-hack/plugins");
        project.getExtensions().add("pluginsDir", pluginsDir);
        project.getTasks().register("createPluginsDir", EmptyDirTask.class, t -> {
            t.setDir(pluginsDir);
            t.setDirMode(0755);
        });

        File jvmOptionsDir = new File(project.getBuildDir(), "jvm-options-hack/jvm.options.d");
        project.getExtensions().add("jvmOptionsDir", jvmOptionsDir);
        project.getTasks().register("createJvmOptionsDir", EmptyDirTask.class, t -> {
            t.setDir(jvmOptionsDir);
            t.setDirMode(0750);
        });
    }

    private static String archiveTaskToSubprojectName(String taskName) {
        return archiveToSubprojectName(taskName).substring("build".length() + 1);
    }

    private static String archiveToSubprojectName(String taskName) {
        return taskName.replaceAll("[A-Z]", "-$0").toLowerCase();
    }
}
