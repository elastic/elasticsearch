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
package org.elasticsearch;

import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.Action;
import org.gradle.api.Project;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.WorkResult;
import org.gradle.process.ExecResult;
import org.gradle.process.ExecSpec;
import org.gradle.process.JavaExecSpec;

import java.io.File;
import java.nio.file.Path;

/**
 * Bridge a gap until Gradle offers service injection for plugins.
 *
 * In a future release Gradle will offer service injection, this adapter plays that role until that time.
 * It exposes the service methods that are part of the public API as the classes implementing them are not.
 * Today service injection is <a href="https://github.com/gradle/gradle/issues/2363">not available</a> for
 * extensions.
 *
 * Everything exposed here must be thread safe. That is the very reason why project is not passed in directly.
 */
public class GradleServicesAdapter {

    private final Project project;

    public GradleServicesAdapter(Project project) {
        this.project = project;
    }

    public static GradleServicesAdapter getInstance(Project project) {
        return new GradleServicesAdapter(project);
    }

    public WorkResult copy(Action<? super CopySpec> action) {
        return project.copy(action);
    }

    public WorkResult sync(Action<? super CopySpec> action) {
        return project.sync(action);
    }

    public ExecResult javaexec(Action<? super JavaExecSpec> action) {
        return project.javaexec(action);
    }

    public FileTree zipTree(File zipPath) {
        return project.zipTree(zipPath);
    }

    public FileCollection fileTree(File dir) {
        return project.fileTree(dir);
    }

    public void loggedExec(Action<ExecSpec> action) {
        LoggedExec.exec(project, action);
    }

    public void delete(Path path) {
        project.delete(path.toFile());
    }
}
