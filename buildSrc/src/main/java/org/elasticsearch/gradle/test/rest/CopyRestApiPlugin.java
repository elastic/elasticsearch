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

//TODO: clean up this doc!
/**
 * <p>
 * Gradle plugin to help configure {@link CopyRestApiTask}'s that will copy the artifacts needed for the Rest API spec and YAML tests.
 * </p>
 * <p>Copies the files needed for the Rest YAML tests to the current projects test resources output directory.
 * This is intended to be be used from {@link CopyRestApiPlugin} since the plugin wires up the needed
 * configurations and custom extensions.
 * </p>
 * <p>This task supports copying either the Rest YAML tests (.yml), or the Rest API specification (.json).</p>
 * <br>
 * <strong>Rest API specification:</strong> <br>
 * When the {@link CopyRestApiPlugin} has been applied this task will automatically copy the Rest API specification
 * if there are any Rest YAML tests present (either in source, or output) or if `restApi.includeCore` or `restApi.includeXpack` has been
 * explicitly declared through the 'restResources' extension. <br>
 * This task supports copying only a subset of the Rest API specification through the use of the custom extension.<br>
 * <i>For example:</i>
 * <pre>
 * restResources {
 *   restApi {
 *     includeXpack 'enrich'
 *   }
 * }
 * </pre>
 * Will copy any of the the x-pack specs that start with enrich*. The core API specs will also be copied iff the project also has
 * Rest YAML tests. To help optimize the build cache, it is recommended to explicitly declare which specs your project depends on.
 * <i>For example:</i>
 * <pre>
 * restResources {
 *   restApi {
 *     includeCore 'index', 'cat'
 *     includeXpack 'enrich'
 *   }
 * }
 * </pre>
 * <br>
 * <strong>Rest YAML tests :</strong> <br>
 * When the {@link CopyRestApiPlugin} has been applied this task can copy the Rest YAML tests iff explicitly configured with
 * `includeCore` or `includeXpack` through the `restResources.restTests` extension.
 * <i>For example:</i>
 * <pre>
 * restResources {
 *   restTests {
 *     includeXpack 'graph'
 *   }
 * }
 * </pre>
 * Will copy any of the the x-pack tests that start with graph.
 *
 * @see CopyRestApiTask
 * @see CopyRestTestsTask
 */
public class CopyRestApiPlugin implements Plugin<Project> {

    private static final String EXTENSION_NAME = "restResources";

    @Override
    public void apply(Project project) {
        RestResourcesExtension extension = project.getExtensions().create(EXTENSION_NAME, RestResourcesExtension.class);

        Provider<CopyRestTestsTask> copyRestYamlTestTask = project.getTasks()
            .register("copyYamlTestsTask", CopyRestTestsTask.class, task -> {
                task.includeCore.set(extension.restTests.getIncludeCore());
                task.includeXpack.set(extension.restTests.getIncludeXpack());
                task.copyTo = "rest-api-spec/test";
                task.coreConfig = project.getConfigurations().create("restTest");
                if (BuildParams.isInternal()) {
                    Dependency dependency = project.getDependencies()
                        .project(Map.of("path", ":rest-api-spec", "configuration", "restTests"));
                    project.getDependencies().add(task.coreConfig.getName(), dependency);

                    task.xpackConfig = project.getConfigurations().create("restXpackTest");
                    dependency = project.getDependencies().project(Map.of("path", ":x-pack:plugin", "configuration", "restXpackTests"));
                    project.getDependencies().add(task.xpackConfig.getName(), dependency);
                    task.dependsOn(task.xpackConfig);

                } else {
                    Dependency dependency = project.getDependencies()
                        .create("org.elasticsearch:rest-api-spec:" + VersionProperties.getElasticsearch());
                    project.getDependencies().add(task.coreConfig.getName(), dependency);
                }
                task.dependsOn(task.coreConfig);
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

                    task.xpackConfig = project.getConfigurations().create("restXpackSpec");
                    dependency = project.getDependencies().project(Map.of("path", ":x-pack:plugin", "configuration", "restXpackSpecs"));
                    project.getDependencies().add(task.xpackConfig.getName(), dependency);
                    task.dependsOn(task.xpackConfig);
                } else {
                    Dependency dependency = project.getDependencies()
                        .create("org.elasticsearch:rest-api-spec:" + VersionProperties.getElasticsearch());
                    project.getDependencies().add(task.coreConfig.getName(), dependency);
                }
                task.dependsOn(task.coreConfig);
            });

        project.getTasks().named("processTestResources").configure(t -> t.dependsOn(copyRestYamlSpecTask));
    }
}
