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
package org.elasticsearch.gradle.test.rest;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.info.BuildParams;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.provider.Provider;

import java.util.Map;

/**
 * Gradle plugin to help configure {@link CopyRestApiTask}'s that will copy the artifacts needed for the Rest API spec and YAML tests.
 * @see CopyRestApiTask
 */
public class CopyRestApiPlugin implements Plugin<Project> {

    private static final String EXTENSION_NAME = "restResources";

    @Override
    public void apply(Project project) {
        RestResourcesExtension extension = project.getExtensions().create(EXTENSION_NAME, RestResourcesExtension.class);

        Provider<CopyRestApiTask> copyRestYamlTestTask = project.getTasks().register("copyYamlTestsTask", CopyRestApiTask.class, task -> {
            task.includeCore.set(extension.restTests.getIncludeCore());
            task.includeXpack.set(extension.restTests.getIncludeXpack());
            task.copyTo = "rest-api-spec/test";
            task.coreConfig = project.getConfigurations().create("restTest");
            if (BuildParams.isInternal()) {
                Dependency dependency = project.getDependencies().project(Map.of("path", ":rest-api-spec", "configuration", "restTests"));
                project.getDependencies().add(task.coreConfig.getName(), dependency);
            } else {
                Dependency dependency = project.getDependencies()
                    .create("org.elasticsearch:rest-api-spec:" + VersionProperties.getElasticsearch());
                project.getDependencies().add(task.coreConfig.getName(), dependency);
            }
            task.dependsOn(task.coreConfig);

            task.xpackConfig = project.getConfigurations().create("restXpackTest");
            Dependency dependency = project.getDependencies().project(Map.of("path", ":x-pack:plugin", "configuration", "restXpackTests"));
            project.getDependencies().add(task.xpackConfig.getName(), dependency);
            task.dependsOn(task.xpackConfig);
        });

        Provider<CopyRestApiTask> copyRestYamlSpecTask = project.getTasks()
            .register("copyRestApiSpecsTask", CopyRestApiTask.class, task -> {
                task.includeCore.set(extension.restApi.getIncludeCore());
                task.includeXpack.set(extension.restApi.getIncludeXpack());
                task.copyTo = "rest-api-spec/api";
                task.dependsOn(copyRestYamlTestTask);
                task.coreConfig = project.getConfigurations().create("restSpec");
                if (BuildParams.isInternal()) {
                    Dependency dependency = project.getDependencies()
                        .project(Map.of("path", ":rest-api-spec", "configuration", "restSpecs"));
                    project.getDependencies().add(task.coreConfig.getName(), dependency);
                } else {
                    Dependency dependency = project.getDependencies()
                        .create("org.elasticsearch:rest-api-spec:" + VersionProperties.getElasticsearch());
                    project.getDependencies().add(task.coreConfig.getName(), dependency);
                }
                task.dependsOn(task.coreConfig);

                task.xpackConfig = project.getConfigurations().create("restXpackSpec");
                Dependency dependency = project.getDependencies()
                    .project(Map.of("path", ":x-pack:plugin", "configuration", "restXpackSpecs"));
                project.getDependencies().add(task.xpackConfig.getName(), dependency);
                task.dependsOn(task.xpackConfig);
            });

        project.getTasks().named("processTestResources").configure(t -> t.dependsOn(copyRestYamlSpecTask));
    }
}
