/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.rest.compat;

import org.elasticsearch.gradle.ElasticsearchJavaPlugin;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.test.RestIntegTestTask;
import org.elasticsearch.gradle.test.RestTestBasePlugin;
import org.elasticsearch.gradle.test.rest.CopyRestApiTask;
import org.elasticsearch.gradle.test.rest.CopyRestTestsTask;
import org.elasticsearch.gradle.test.rest.RestResourcesExtension;
import org.elasticsearch.gradle.test.rest.RestResourcesPlugin;
import org.elasticsearch.gradle.test.rest.RestTestUtil;
import org.elasticsearch.gradle.test.rest.YamlRestTestPlugin;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.testclusters.TestDistribution;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import static org.elasticsearch.gradle.test.rest.RestTestUtil.createTestCluster;
import static org.elasticsearch.gradle.test.rest.RestTestUtil.setupDependencies;

/**
 * Apply this plugin to run the YAML based REST tests from a prior major version against this version's cluster.
 */
public class YamlRestCompatTestPlugin implements Plugin<Project> {

    public static final String REST_COMPAT_CHECK_TASK_NAME = "checkRestCompat";
    public static final String SOURCE_SET_NAME = "yamlRestCompatTest";
    private static final Path RELATIVE_API_PATH = Path.of("rest-api-spec/api");
    private static final Path RELATIVE_TEST_PATH = Path.of("rest-api-spec/test");
    private static final Path RELATIVE_REST_API_RESOURCES = Path.of("rest-api-spec/src/main/resources");
    private static final Path RELATIVE_REST_XPACK_RESOURCES = Path.of("x-pack/plugin/src/test/resources");
    private static final Path RELATIVE_REST_PROJECT_RESOURCES = Path.of("src/yamlRestTest/resources");
    private static final String TEST_INTERMEDIATE_DIR_NAME = "v"
        + (Version.fromString(VersionProperties.getVersions().get("elasticsearch")).getMajor() - 1)
        + "restTests";

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
            .register("copyRestCompatApiTask", CopyRestApiTask.class, task -> {
                task.dependsOn(bwcMinorConfig);
                task.setCoreConfig(bwcMinorConfig);
                task.setXpackConfig(bwcMinorConfig);
                task.setAdditionalConfig(bwcMinorConfig);
                task.getIncludeCore().set(extension.getRestApi().getIncludeCore());
                task.getIncludeXpack().set(extension.getRestApi().getIncludeXpack());
                task.setOutputResourceDir(yamlCompatTestSourceSet.getOutput().getResourcesDir());
                task.setSourceResourceDir(
                    yamlCompatTestSourceSet.getResources()
                        .getSrcDirs()
                        .stream()
                        .filter(f -> f.isDirectory() && f.getName().equals("resources"))
                        .findFirst()
                        .orElse(null)
                );
                task.setSkipHasRestTestCheck(true);
                task.setCoreConfigToFileTree(
                    config -> project.fileTree(
                        config.getSingleFile().toPath().resolve(RELATIVE_REST_API_RESOURCES).resolve(RELATIVE_API_PATH)
                    )
                );
                task.setXpackConfigToFileTree(
                    config -> project.fileTree(
                        config.getSingleFile().toPath().resolve(RELATIVE_REST_XPACK_RESOURCES).resolve(RELATIVE_API_PATH)
                    )
                );
                task.setAdditionalConfigToFileTree(
                    config -> project.fileTree(
                        getCompatProjectPath(project, config.getSingleFile().toPath()).resolve(RELATIVE_REST_PROJECT_RESOURCES)
                            .resolve(RELATIVE_API_PATH)
                    )
                );
                task.onlyIf(t -> isEnabled(project));
            });

        // copy compatible rest tests
        Provider<CopyRestTestsTask> copyCompatYamlTestTask = project.getTasks()
            .register("copyRestCompatTestTask", CopyRestTestsTask.class, task -> {
                task.dependsOn(bwcMinorConfig);
                task.setCoreConfig(bwcMinorConfig);
                task.setXpackConfig(bwcMinorConfig);
                task.setAdditionalConfig(bwcMinorConfig);
                task.getIncludeCore().set(extension.getRestTests().getIncludeCore());
                task.getIncludeXpack().set(extension.getRestTests().getIncludeXpack());
                File resourceDir = yamlCompatTestSourceSet.getOutput().getResourcesDir();
                File intermediateDir = new File(resourceDir, TEST_INTERMEDIATE_DIR_NAME);
                task.setOutputResourceDir(intermediateDir);
                task.setCoreConfigToFileTree(
                    config -> project.fileTree(
                        config.getSingleFile().toPath().resolve(RELATIVE_REST_API_RESOURCES).resolve(RELATIVE_TEST_PATH)
                    )
                );
                task.setXpackConfigToFileTree(
                    config -> project.fileTree(
                        config.getSingleFile().toPath().resolve(RELATIVE_REST_XPACK_RESOURCES).resolve(RELATIVE_TEST_PATH)
                    )
                );
                task.setAdditionalConfigToFileTree(
                    config -> project.fileTree(
                        getCompatProjectPath(project, config.getSingleFile().toPath()).resolve(RELATIVE_REST_PROJECT_RESOURCES)
                            .resolve(RELATIVE_TEST_PATH)
                    )
                );
                task.dependsOn(copyCompatYamlSpecTask);
                task.onlyIf(t -> isEnabled(project));
            });

        // transform the copied tests task
        TaskProvider<RestCompatTestTransformTask> transformCompatTestTask = project.getTasks()
            .register("transformCompatTests", RestCompatTestTransformTask.class, task -> {
                task.dependsOn(copyCompatYamlTestTask);
                task.dependsOn(yamlCompatTestSourceSet.getProcessResourcesTaskName());
                File resourceDir = yamlCompatTestSourceSet.getOutput().getResourcesDir();
                File intermediateDir = new File(resourceDir, TEST_INTERMEDIATE_DIR_NAME);
                task.setInput(project.files(new File(intermediateDir, RELATIVE_TEST_PATH.toString())));
                task.setOutput(new File(resourceDir, RELATIVE_TEST_PATH.toString()));

                task.onlyIf(t -> isEnabled(project));
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
            testTask.dependsOn(transformCompatTestTask);
            testTask.onlyIf(t -> isEnabled(project));
        });

        // setup the dependencies
        setupDependencies(project, yamlCompatTestSourceSet);

        // setup IDE
        GradleUtils.setupIdeForTestSourceSet(project, yamlCompatTestSourceSet);

        // add a lifecycle task to allow for a possible future additional rest compatibility without needing to change task names
        TaskProvider<Task> checkRestCompatTask = project.getTasks().register(REST_COMPAT_CHECK_TASK_NAME, (thisCheckTask) -> {
            thisCheckTask.setDescription("Runs all REST compatibility checks.");
            thisCheckTask.setGroup("verification");
        });

        // wire the lifecycle task into the main check task
        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(checkRestCompatTask));

        // wire the yamlRestCompatTest into the custom lifecycle task
        project.getTasks().named(REST_COMPAT_CHECK_TASK_NAME).configure(check -> check.dependsOn(yamlRestCompatTestTask));

    }

    private boolean isEnabled(Project project) {
        Object bwcEnabled = project.getExtensions().getExtraProperties().getProperties().get("bwc_tests_enabled");
        return bwcEnabled == null || (Boolean) bwcEnabled;
    }

    // TODO: implement custom extension that allows us move around of the projects between major versions and still find them
    private Path getCompatProjectPath(Project project, Path checkoutDir) {
        return checkoutDir.resolve(project.getPath().replaceFirst(":", "").replace(":", File.separator));
    }
}
