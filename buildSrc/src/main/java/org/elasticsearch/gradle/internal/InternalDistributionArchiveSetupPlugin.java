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
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ConfigurationPublications;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.internal.artifacts.ArtifactAttributes;
import org.gradle.api.internal.artifacts.ConfigurationVariantInternal;
import org.gradle.api.internal.artifacts.publish.AbstractPublishArtifact;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.tasks.AbstractCopyTask;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;
import org.gradle.api.tasks.bundling.Compression;

import java.io.File;
import java.util.Collections;
import java.util.Date;

import static org.gradle.util.GUtil.toCamelCase;

/**
 * Common configurations for elasticsearch distribution archives
 */
public class InternalDistributionArchiveSetupPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(BasePlugin.class);
        registerEmptyDirectoryTasks(project);
        registerSubProjectArtifacts(project);
        configureGeneralDefaults(project);
        configureTarDefaults(project);
    }

    private void registerSubProjectArtifacts(Project project) {
        // TODO look into avoiding afterEvaluate
        project.afterEvaluate(project1 -> project1.subprojects(sub -> {
            String buildArchiveTaskName = "build" + toCamelCase(sub.getName());
            sub.getArtifacts().add("default", project1.getTasks().named(buildArchiveTaskName));
            String copyDistributionTaskName = buildArchiveTaskName.substring(0, buildArchiveTaskName.length() - 3);

            // TODO do this in a task avoidance api friendly way
            Task copyTask = project1.getTasks().findByName(copyDistributionTaskName);
            // TODO should not be required once completetly configured for each archive
            if (copyTask != null) {
                Configuration defaultConfiguration = sub.getConfigurations().getByName("default");
                ConfigurationPublications publications = defaultConfiguration.getOutgoing();
                ConfigurationVariantInternal variant = (ConfigurationVariantInternal) publications.getVariants().maybeCreate("directory");
                variant.getAttributes().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
                variant.artifactsProvider(() -> Collections.singletonList(new DirectoryPublishArtifact(copyTask)));
            }
        }));
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
            String subdir = buildTaskToSubprojectName(t);
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

    private String buildTaskToSubprojectName(AbstractArchiveTask t) {
        return t.getName().substring("build".length()).replaceAll("[A-Z]", "-$0").toLowerCase().substring(1);
    }

    private static class DirectoryPublishArtifact extends AbstractPublishArtifact {

        private final Task providerTask;

        DirectoryPublishArtifact(Task providerTask) {
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
            return providerTask.getOutputs().getFiles().getSingleFile();
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
