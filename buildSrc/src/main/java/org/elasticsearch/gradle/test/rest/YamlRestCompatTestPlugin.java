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
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Zip;

import java.io.File;
import java.nio.file.Path;

import static org.elasticsearch.gradle.test.rest.RestTestUtil.createTestCluster;
import static org.elasticsearch.gradle.test.rest.RestTestUtil.registerTask;
import static org.elasticsearch.gradle.test.rest.RestTestUtil.setupDependencies;

/**
 * Apply this plugin to run the YAML based REST tests.
 */
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

        // create source set
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet yamlCompatTestSourceSet = sourceSets.create(SOURCE_SET_NAME);
        SourceSet yamlTestSourceSet = sourceSets.getByName(YamlRestTestPlugin.SOURCE_SET_NAME);
        GradleUtils.extendSourceSet(project, YamlRestTestPlugin.SOURCE_SET_NAME, SOURCE_SET_NAME);

        // create the test cluster container, and always use the default distribution
        ElasticsearchCluster testCluster = createTestCluster(project, yamlCompatTestSourceSet);
        testCluster.setTestDistribution(TestDistribution.DEFAULT);

        // Get a reference to the checkout directory for ":distribution:bwc:minor:checkoutBwcBranch"
        //TODO: this is pretty fragile and we need to eventually want to test against multiple minor versions, however to do so we will
        // need to support to checking out the source branches per version provide a less fragile way to get a reference to the checkoutDir
        int priorMajorVersion = VersionProperties.getElasticsearchVersion().getMajor() - 1;
        final Path checkoutDir = project.findProject(":distribution:bwc:minor").getBuildDir().toPath()
                            .resolve("bwc").resolve("checkout-" + priorMajorVersion + ".x");

        // copy the api from the checked out source to the compatible sourceset
        TaskProvider<Copy> copyApis = project.getTasks().register(SOURCE_SET_NAME + "#copyApis", Copy.class, copy -> {
            copy.from(checkoutDir.resolve("rest-api-spec/src/main/resources").resolve(RELATIVE_API_PATH));
            //TODO: prefer to read from the compat rest resources
            //copy xpack api's
            if (project.getPath().startsWith(":x-pack")) {
                copy.from(checkoutDir.resolve("x-pack/plugin/src/test/resources").resolve(RELATIVE_API_PATH));
            }
            // copy any module or plugin test and APIs
            if (project.getPath().startsWith(":modules")
                || project.getPath().startsWith(":plugins")
                || project.getPath().startsWith(":x-pack:plugin:")) {
                //TODO: cross check against hard coded list of where to find prior version.
                copy.from(checkoutDir.resolve(project.getPath().replaceFirst(":", "").replace(":", File.separator))
                    .resolve("src/yamlRestTest/resources").resolve(RELATIVE_API_PATH));
            }
            copy.into(yamlCompatTestSourceSet.getOutput().getResourcesDir().toPath().resolve(RELATIVE_API_PATH));
              copy.dependsOn(":distribution:bwc:minor:checkoutBwcBranch");
        });

        // copy the tests from the checked out source to the compatible sourceset
        TaskProvider<Copy> copyTests = project.getTasks().register(SOURCE_SET_NAME + "#copyTests", Copy.class, copy -> {
            //copy core tests
            if (project.getPath().equalsIgnoreCase(":rest-api-spec")) {
                copy.from(checkoutDir.resolve("rest-api-spec/src/main/resources").resolve(RELATIVE_TEST_PATH));
            }
            // copy module or plugin tests
            if (project.getPath().startsWith(":modules")
                || project.getPath().startsWith(":plugins")
                || project.getPath().startsWith(":x-pack:plugin:")) { // trailing colon intentional to disambiguate
                copy.from(checkoutDir
                    .resolve(project.getPath().replaceFirst(":", "").replace(":", File.separator))
                    .resolve("src/yamlRestTest/resources").resolve(RELATIVE_TEST_PATH));
            }
            //copy xpack tests
            if (project.getPath().equalsIgnoreCase(":x-pack:plugin")) {
                copy.from(checkoutDir.resolve("x-pack/plugin/src/test/resources").resolve(RELATIVE_TEST_PATH));
            }
            copy.into(yamlCompatTestSourceSet.getOutput().getResourcesDir().toPath().resolve(RELATIVE_TEST_PATH));
            copy.dependsOn(copyApis);
        });

        //TODO: also copy configuration from the copyRestResources extention, and provide a compatible override

        // setup the yamlRestTest task
        Provider<RestIntegTestTask> yamlRestCompatTestTask = RestTestUtil.registerTask(project, yamlCompatTestSourceSet);
        project.getTasks().withType(RestIntegTestTask.class).named(SOURCE_SET_NAME).configure(testTask -> {
            //Use test runner and classpath from "normal" yaml source set
            testTask.setTestClassesDirs(yamlTestSourceSet.getOutput().getClassesDirs());
            testTask.setClasspath(yamlTestSourceSet.getRuntimeClasspath()
                //remove the "normal" api and tests
                .minus(project.files(yamlTestSourceSet.getOutput().getResourcesDir()))
                // add any additional classes/resources from the compatible source set
                // the api and tests are copied to the compatible source set
                .plus(yamlCompatTestSourceSet.getRuntimeClasspath())
            );
            // run compatibility tests after "normal" tests
            testTask.mustRunAfter(project.getTasks().named(YamlRestTestPlugin.SOURCE_SET_NAME));
            testTask.dependsOn(copyTests);
        });

        // setup the dependencies
        setupDependencies(project, yamlCompatTestSourceSet);

        // setup IDE
        GradleUtils.setupIdeForTestSourceSet(project, yamlCompatTestSourceSet);

        // wire this task into check
        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(yamlRestCompatTestTask));
    }
}
