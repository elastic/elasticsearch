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
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;
import org.gradle.api.tasks.bundling.Compression;

import java.io.File;

/**
 * Common configurations for elasticsearch distribution archives
 * */
public class InternalDistributionArchiveSetupPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(BasePlugin.class);
        registerEmptyDirectoryTasks(project);
        configureArchiveDefaults(project);
        configureTarDefaults(project);
    }

    private void configureArchiveDefaults(Project project) {
        // common config across all archives
        project.getTasks().withType(AbstractArchiveTask.class).configureEach(t -> {
            t.dependsOn(project.getTasks().withType(EmptyDirTask.class));
            String subdir = buildTaskToSubprojectName(t);
            t.getDestinationDirectory().set(project.file(subdir + "/build/distributions"));
            t.getArchiveBaseName().set(subdir.contains("oss") ? "elasticsearch-oss" : "elasticsearch");
            t.setDirMode(0755);
            t.setFileMode(0644);
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
}
