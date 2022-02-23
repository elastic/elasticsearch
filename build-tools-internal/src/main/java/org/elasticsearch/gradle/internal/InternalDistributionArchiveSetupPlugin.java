/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.tasks.AbstractCopyTask;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;
import org.gradle.api.tasks.bundling.Compression;
import org.gradle.api.tasks.bundling.Zip;

import java.io.File;

import static org.elasticsearch.gradle.internal.conventions.GUtils.capitalize;

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
public class InternalDistributionArchiveSetupPlugin implements InternalPlugin {

    public static final String DEFAULT_CONFIGURATION_NAME = "default";
    public static final String EXTRACTED_CONFIGURATION_NAME = "extracted";
    public static final String COMPOSITE_CONFIGURATION_NAME = "composite";
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
            var explodedDist = tasks.register(copyDistributionTaskName, Sync.class, sync -> sync.into(subProjectDir + "/build/install/"));
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
                var extractedConfiguration = sub.getConfigurations().create(EXTRACTED_CONFIGURATION_NAME);
                extractedConfiguration.setCanBeResolved(false);
                extractedConfiguration.getAttributes()
                    .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
                sub.getArtifacts().add(EXTRACTED_CONFIGURATION_NAME, distributionArchive.getExpandedDistTask());
                // The "composite" configuration is specifically used for resolving transformed artifacts in an included build
                var compositeConfiguration = sub.getConfigurations().create(COMPOSITE_CONFIGURATION_NAME);
                compositeConfiguration.setCanBeResolved(false);
                compositeConfiguration.getAttributes()
                    .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
                compositeConfiguration.getAttributes().attribute(Attribute.of("composite", Boolean.class), true);
                sub.getArtifacts().add(COMPOSITE_CONFIGURATION_NAME, distributionArchive.getArchiveTask());
                sub.getTasks().register("extractedAssemble", task ->
                // We keep extracted configuration resolvable false to keep
                // resolveAllDependencies simple so we rely only on its build dependencies here.
                task.dependsOn(extractedConfiguration.getAllArtifacts().getBuildDependencies()));
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
