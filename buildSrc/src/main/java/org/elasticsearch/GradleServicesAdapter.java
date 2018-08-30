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

import org.gradle.api.Action;
import org.gradle.api.Project;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.WorkResult;

import java.io.File;

/*
 * In a future release Gradle will offer service injection, this is a temporary measure until that that time.
 * It exposes the service methods that are part of the public API as the classes implementing them are not.
 * Today service injection is <a href="https://github.com/gradle/gradle/issues/2363">not available</a> for
 * plugins.
 *
 */
public class GradleServicesAdapter {
    private final Project project;

    private GradleServicesAdapter(Project project) {
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


    public FileTree zipTree(File zipPath) {
        return project.zipTree(zipPath);
    }

    public FileCollection fileTree(File dir) {
        return project.fileTree(dir);
    }
}
