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
import org.gradle.api.internal.artifacts.ArtifactAttributes;
import org.gradle.api.internal.artifacts.ConfigurationVariantInternal;
import org.gradle.api.internal.artifacts.publish.AbstractPublishArtifact;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.tasks.AbstractCopyTask;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;
import org.gradle.api.tasks.bundling.Compression;
import org.gradle.api.tasks.bundling.Zip;

import java.io.File;
import java.util.Collections;
import java.util.Date;

/**
 * Common configurations for elasticsearch distribution archives
 */
public class InternalDistributionArchiveSetupPlugin implements Plugin<Project> {

    private NamedDomainObjectContainer<DistributionArchive> container;

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(BasePlugin.class);
        registerDistributionArchivesExtension(project);
        registerEmptyDirectoryTasks(project);
        registerSubProjectArtifacts(project);
        configureGeneralDefaults(project);
        configureTarDefaults(project);
    }

    private void registerDistributionArchivesExtension(Project project) {
        container = project.container(DistributionArchive.class, name -> {
            var subProjectDir = buildTaskToSubprojectName(name);
            var copyDistributionTaskName = name.substring(0, name.length() - 3);
            var explodedDist = project.getTasks()
                .register(copyDistributionTaskName, Copy.class, copy -> copy.into(subProjectDir + "/build/install/"));
            return name.endsWith("Tar")
                ? new DistributionArchive(project.getTasks().register(name, SymbolicLinkPreservingTar.class), explodedDist, name)
                : new DistributionArchive(project.getTasks().register(name, Zip.class), explodedDist, name);
        });

        project.getExtensions().add("distribution_archives", container);
    }

    private void registerSubProjectArtifacts(Project project) {
        container.whenObjectAdded(distributionArchive -> {
            var subProjectName = buildTaskToSubprojectName(distributionArchive.getArchiveTask().getName());
            project.project(subProjectName, sub -> {
                sub.getPlugins().apply("base");
                sub.getArtifacts().add("default", distributionArchive.getArchiveTask());
                var explodedArchiveTask = distributionArchive.getExplodedArchiveTask();
                var defaultConfiguration = sub.getConfigurations().getByName("default");
                var publications = defaultConfiguration.getOutgoing();
                var variant = (ConfigurationVariantInternal) publications.getVariants().maybeCreate("directory");
                variant.getAttributes().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
                variant.artifactsProvider(() -> Collections.singletonList(new DirectoryPublishArtifact(explodedArchiveTask)));
            });
        });
    }

    private void configureGeneralDefaults(Project project) {
        // common config across all copy / archive tasks
        project.getTasks().withType(AbstractCopyTask.class).configureEach(t -> {
            t.dependsOn(project.getTasks().withType(EmptyDirTask.class));
            t.setIncludeEmptyDirs(true);
            t.setDirMode(0755);
            t.setFileMode(0644);
        });

        // common config across all archives
        project.getTasks().withType(AbstractArchiveTask.class).configureEach(t -> {
            String subdir = buildTaskToSubprojectName(t.getName());
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
        project.getExtensions().add("logsDir", new File(project.getBuildDir(), "logs-hack/logs"));
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

    private String buildTaskToSubprojectName(String taskName) {
        return taskName.substring("build".length()).replaceAll("[A-Z]", "-$0").toLowerCase().substring(1);
    }

    private static class DirectoryPublishArtifact extends AbstractPublishArtifact {

        private final TaskProvider<Copy> providerTask;

        DirectoryPublishArtifact(TaskProvider<Copy> providerTask) {
            super(providerTask);
            this.providerTask = providerTask;
        }

        @Override
        public String getName() {
            return providerTask.getName();
        }

        @Override
        public String getExtension() {
            return "";
        }

        @Override
        public String getType() {
            return ArtifactTypeDefinition.DIRECTORY_TYPE;
        }

        @Override
        public String getClassifier() {
            return null;
        }

        @Override
        public File getFile() {
            return providerTask.get().getOutputs().getFiles().getSingleFile();
        }

        @Override
        public Date getDate() {
            return null;
        }

        @Override
        public boolean shouldBePublished() {
            return false;
        }
    }
}
