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
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.test.RestIntegTestTask;
import org.elasticsearch.gradle.test.RestTestBasePlugin;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.testclusters.TestDistribution;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;

import java.io.File;
import java.nio.file.Path;

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

        // TODO: once https://github.com/elastic/elasticsearch/pull/62473 lands refactor this to reference the checkoutDir as an artifact
        int priorMajorVersion = VersionProperties.getElasticsearchVersion().getMajor() - 1;
        final Path checkoutDir = project.findProject(":distribution:bwc:minor")
            .getBuildDir()
            .toPath()
            .resolve("bwc")
            .resolve("checkout-" + priorMajorVersion + ".x");

        // copy compatible rest specs
        Configuration compatSpec = project.getConfigurations().create("compatSpec");
        Configuration xpackCompatSpec = project.getConfigurations().create("xpackCompatSpec");
        Configuration additionalCompatSpec = project.getConfigurations().create("additionalCompatSpec");
        Provider<CopyRestApiTask> copyCompatYamlSpecTask = project.getTasks()
            .register("copyRestApiCompatSpecsTask", CopyRestApiTask.class, task -> {
                task.includeCore.set(extension.restApi.getIncludeCore());
                task.includeXpack.set(extension.restApi.getIncludeXpack());
                task.sourceSetName = SOURCE_SET_NAME;
                task.skipHasRestTestCheck = true;
                task.coreConfig = compatSpec;
                project.getDependencies()
                    .add(
                        task.coreConfig.getName(),
                        project.files(checkoutDir.resolve("rest-api-spec/src/main/resources").resolve(RELATIVE_API_PATH))
                    );
                task.xpackConfig = xpackCompatSpec;
                project.getDependencies()
                    .add(
                        task.xpackConfig.getName(),
                        project.files(checkoutDir.resolve("x-pack/plugin/src/test/resources").resolve(RELATIVE_API_PATH))
                    );
                task.additionalConfig = additionalCompatSpec;
                // per project can define custom specifications
                project.getDependencies()
                    .add(
                        task.additionalConfig.getName(),
                        project.files(
                            getCompatProjectPath(project, checkoutDir).resolve("src/yamlRestTest/resources").resolve(RELATIVE_API_PATH)
                        )
                    );
                task.dependsOn(task.coreConfig);
                task.dependsOn(task.xpackConfig);
                task.dependsOn(task.additionalConfig);
                task.dependsOn(":distribution:bwc:minor:checkoutBwcBranch");
            });

        // copy compatible rest tests
        Configuration compatTest = project.getConfigurations().create("compatTest");
        Configuration xpackCompatTest = project.getConfigurations().create("xpackCompatTest");
        Configuration additionalCompatTest = project.getConfigurations().create("additionalCompatTest");
        Provider<CopyRestTestsTask> copyCompatYamlTestTask = project.getTasks()
            .register("copyRestApiCompatTestTask", CopyRestTestsTask.class, task -> {
                task.includeCore.set(extension.restTests.getIncludeCore());
                task.includeXpack.set(extension.restTests.getIncludeXpack());
                task.sourceSetName = SOURCE_SET_NAME;
                task.coreConfig = compatTest;
                project.getDependencies()
                    .add(
                        task.coreConfig.getName(),
                        project.files(checkoutDir.resolve("rest-api-spec/src/main/resources").resolve(RELATIVE_TEST_PATH))
                    );
                task.xpackConfig = xpackCompatTest;
                project.getDependencies()
                    .add(
                        task.xpackConfig.getName(),
                        project.files(checkoutDir.resolve("x-pack/plugin/src/test/resources").resolve(RELATIVE_TEST_PATH))
                    );
                task.additionalConfig = additionalCompatTest;
                project.getDependencies()
                    .add(
                        task.additionalConfig.getName(),
                        project.files(
                            getCompatProjectPath(project, checkoutDir).resolve("src/yamlRestTest/resources").resolve(RELATIVE_TEST_PATH)
                        )
                    );
                task.dependsOn(task.coreConfig);
                task.dependsOn(task.xpackConfig);
                task.dependsOn(task.additionalConfig);
                task.dependsOn(":distribution:bwc:minor:checkoutBwcBranch");
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
