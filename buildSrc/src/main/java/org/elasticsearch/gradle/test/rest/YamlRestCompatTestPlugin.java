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

package org.elasticsearch.gradle.test.rest;

import org.elasticsearch.gradle.ElasticsearchJavaPlugin;
import org.elasticsearch.gradle.test.RestIntegTestTask;
import org.elasticsearch.gradle.test.RestTestBasePlugin;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.testclusters.TestDistribution;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import static org.elasticsearch.gradle.test.rest.RestTestUtil.createTestCluster;
import static org.elasticsearch.gradle.test.rest.RestTestUtil.setupDependencies;

/**
 * Apply this plugin to run the YAML based REST tests from a prior major version against this version's cluster.
 */
// TODO: support running tests against multiple prior versions in addition to bwc:minor. To achieve this we will need to create published
// artifacts that include all of the REST tests for the latest 7.x.y releases
public class YamlRestCompatTestPlugin implements Plugin<Project> {

    public static final String SOURCE_SET_NAME = "yamlRestCompatTest";
    private static final Path RELATIVE_API_PATH = Path.of("rest-api-spec/api");
    private static final Path RELATIVE_TEST_PATH = Path.of("rest-api-spec/test");
    private static final Path RELATIVE_REST_API_RESOURCES = Path.of("rest-api-spec/src/main/resources");
    private static final Path RELATIVE_REST_XPACK_RESOURCES = Path.of("x-pack/plugin/src/test/resources");
    private static final Path RELATIVE_REST_PROJECT_RESOURCES = Path.of("src/yamlRestTest/resources");

    @Override
    public void apply(Project project) {

        project.getPluginManager().apply(ElasticsearchJavaPlugin.class);
        project.getPluginManager().apply(TestClustersPlugin.class);
        project.getPluginManager().apply(RestTestBasePlugin.class);
        project.getPluginManager().apply(RestResourcesPlugin.class);
        project.getPluginManager().apply(YamlRestTestPlugin.class);

        RestResourcesExtension extension = project.getExtensions().getByType(RestResourcesExtension.class);

        // create source set
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet yamlCompatTestSourceSet = sourceSets.create(SOURCE_SET_NAME);
        SourceSet yamlTestSourceSet = sourceSets.getByName(YamlRestTestPlugin.SOURCE_SET_NAME);
        GradleUtils.extendSourceSet(project, YamlRestTestPlugin.SOURCE_SET_NAME, SOURCE_SET_NAME);

        // create the test cluster container, and always use the default distribution
        ElasticsearchCluster testCluster = createTestCluster(project, yamlCompatTestSourceSet);
        testCluster.setTestDistribution(TestDistribution.DEFAULT);

        // copy compatible rest specs
        Configuration bwcMinorConfig = project.getConfigurations().create("bwcMinor");
        Dependency bwcMinor = project.getDependencies().project(Map.of("path", ":distribution:bwc:minor", "configuration", "checkout"));
        project.getDependencies().add(bwcMinorConfig.getName(), bwcMinor);

        Provider<CopyRestApiTask> copyCompatYamlSpecTask = project.getTasks()
            .register("copyRestApiCompatSpecsTask", CopyRestApiTask.class, task -> {
                task.dependsOn(bwcMinorConfig);
                task.coreConfig = bwcMinorConfig;
                task.xpackConfig = bwcMinorConfig;
                task.additionalConfig = bwcMinorConfig;
                task.includeCore.set(extension.restApi.getIncludeCore());
                task.includeXpack.set(extension.restApi.getIncludeXpack());
                task.sourceSetName = SOURCE_SET_NAME;
                task.skipHasRestTestCheck = true;
                task.coreConfigToFileTree = config -> project.fileTree(
                    config.getSingleFile().toPath().resolve(RELATIVE_REST_API_RESOURCES).resolve(RELATIVE_API_PATH)
                );
                task.xpackConfigToFileTree = config -> project.fileTree(
                    config.getSingleFile().toPath().resolve(RELATIVE_REST_XPACK_RESOURCES).resolve(RELATIVE_API_PATH)
                );
                task.additionalConfigToFileTree = config -> project.fileTree(
                    getCompatProjectPath(project, config.getSingleFile().toPath()).resolve(RELATIVE_REST_PROJECT_RESOURCES)
                        .resolve(RELATIVE_API_PATH)
                );
            });

        // copy compatible rest tests
        Provider<CopyRestTestsTask> copyCompatYamlTestTask = project.getTasks()
            .register("copyRestApiCompatTestTask", CopyRestTestsTask.class, task -> {
                task.dependsOn(bwcMinorConfig);
                task.coreConfig = bwcMinorConfig;
                task.xpackConfig = bwcMinorConfig;
                task.additionalConfig = bwcMinorConfig;
                task.includeCore.set(extension.restTests.getIncludeCore());
                task.includeXpack.set(extension.restTests.getIncludeXpack());
                task.sourceSetName = SOURCE_SET_NAME;
                task.coreConfigToFileTree = config -> project.fileTree(
                    config.getSingleFile().toPath().resolve(RELATIVE_REST_API_RESOURCES).resolve(RELATIVE_TEST_PATH)
                );
                task.xpackConfigToFileTree = config -> project.fileTree(
                    config.getSingleFile().toPath().resolve(RELATIVE_REST_XPACK_RESOURCES).resolve(RELATIVE_TEST_PATH)
                );
                task.additionalConfigToFileTree = config -> project.fileTree(
                    getCompatProjectPath(project, config.getSingleFile().toPath()).resolve(RELATIVE_REST_PROJECT_RESOURCES)
                        .resolve(RELATIVE_TEST_PATH)
                );
                task.dependsOn(copyCompatYamlSpecTask);
            });

        // setup the yamlRestTest task
        Provider<RestIntegTestTask> yamlRestCompatTestTask = RestTestUtil.registerTask(project, yamlCompatTestSourceSet);
        project.getTasks().withType(RestIntegTestTask.class).named(SOURCE_SET_NAME).configure(testTask -> {
            // Use test runner and classpath from "normal" yaml source set
            testTask.setTestClassesDirs(yamlTestSourceSet.getOutput().getClassesDirs());
            testTask.setClasspath(
                yamlTestSourceSet.getRuntimeClasspath()
                    // remove the "normal" api and tests
                    .minus(project.files(yamlTestSourceSet.getOutput().getResourcesDir()))
                    // add any additional classes/resources from the compatible source set
                    // the api and tests are copied to the compatible source set
                    .plus(yamlCompatTestSourceSet.getRuntimeClasspath())
            );
            // run compatibility tests after "normal" tests
            testTask.mustRunAfter(project.getTasks().named(YamlRestTestPlugin.SOURCE_SET_NAME));
            testTask.dependsOn(copyCompatYamlTestTask);
        });

        // setup the dependencies
        setupDependencies(project, yamlCompatTestSourceSet);

        // setup IDE
        GradleUtils.setupIdeForTestSourceSet(project, yamlCompatTestSourceSet);

        // wire this task into check
        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(yamlRestCompatTestTask));
    }

    // TODO: implement custom extension that allows us move around of the projects between major versions and still find them
    private Path getCompatProjectPath(Project project, Path checkoutDir) {
        return checkoutDir.resolve(project.getPath().replaceFirst(":", "").replace(":", File.separator));
    }
}
