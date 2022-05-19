/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.rest.compat;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.ElasticsearchJavaBasePlugin;
import org.elasticsearch.gradle.internal.test.RestIntegTestTask;
import org.elasticsearch.gradle.internal.test.RestTestBasePlugin;
import org.elasticsearch.gradle.internal.test.rest.CopyRestApiTask;
import org.elasticsearch.gradle.internal.test.rest.CopyRestTestsTask;
import org.elasticsearch.gradle.internal.test.rest.InternalYamlRestTestPlugin;
import org.elasticsearch.gradle.internal.test.rest.RestResourcesExtension;
import org.elasticsearch.gradle.internal.test.rest.RestResourcesPlugin;
import org.elasticsearch.gradle.internal.test.rest.RestTestUtil;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.file.Directory;
import org.gradle.api.file.RelativePath;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.setupYamlRestTestDependenciesDefaults;

/**
 * Apply this plugin to run the YAML based REST tests from a prior major version against this version's cluster.
 */
public class YamlRestCompatTestPlugin implements Plugin<Project> {
    public static final String BWC_MINOR_CONFIG_NAME = "bwcMinor";
    private static final String REST_COMPAT_CHECK_TASK_NAME = "checkRestCompat";
    private static final String COMPATIBILITY_APIS_CONFIGURATION = "restCompatSpecs";
    private static final String COMPATIBILITY_TESTS_CONFIGURATION = "restCompatTests";
    private static final Path RELATIVE_API_PATH = Path.of("rest-api-spec/api");
    private static final Path RELATIVE_TEST_PATH = Path.of("rest-api-spec/test");
    private static final Path RELATIVE_REST_API_RESOURCES = Path.of("rest-api-spec/src/main/resources");
    private static final Path RELATIVE_REST_XPACK_RESOURCES = Path.of("x-pack/plugin/src/test/resources");
    private static final Path RELATIVE_REST_PROJECT_RESOURCES = Path.of("src/yamlRestTest/resources");
    private static final int COMPATIBLE_VERSION = Version.fromString(VersionProperties.getVersions().get("elasticsearch")).getMajor() - 1;
    private static final String SOURCE_SET_NAME = "yamlRestTestV" + COMPATIBLE_VERSION + "Compat";

    @Override
    public void apply(Project project) {

        final Path compatRestResourcesDir = Path.of("restResources").resolve("v" + COMPATIBLE_VERSION);
        final Path compatSpecsDir = compatRestResourcesDir.resolve("yamlSpecs");
        final Path compatTestsDir = compatRestResourcesDir.resolve("yamlTests");

        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);
        project.getPluginManager().apply(TestClustersPlugin.class);
        project.getPluginManager().apply(RestTestBasePlugin.class);
        project.getPluginManager().apply(RestResourcesPlugin.class);
        project.getPluginManager().apply(InternalYamlRestTestPlugin.class);

        RestResourcesExtension extension = project.getExtensions().getByType(RestResourcesExtension.class);

        // create source set
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet yamlCompatTestSourceSet = sourceSets.create(SOURCE_SET_NAME);
        SourceSet yamlTestSourceSet = sourceSets.getByName(InternalYamlRestTestPlugin.SOURCE_SET_NAME);
        GradleUtils.extendSourceSet(project, InternalYamlRestTestPlugin.SOURCE_SET_NAME, SOURCE_SET_NAME);

        // copy compatible rest specs
        Configuration bwcMinorConfig = project.getConfigurations().create(BWC_MINOR_CONFIG_NAME);
        Dependency bwcMinor = project.getDependencies()
            .project(Map.of("path", ":distribution:bwc:maintenance", "configuration", "checkout"));
        project.getDependencies().add(bwcMinorConfig.getName(), bwcMinor);

        Provider<CopyRestApiTask> copyCompatYamlSpecTask = project.getTasks()
            .register("copyRestCompatApiTask", CopyRestApiTask.class, task -> {
                task.dependsOn(bwcMinorConfig);
                task.setConfig(bwcMinorConfig);
                task.setAdditionalConfig(bwcMinorConfig);
                task.getInclude().set(extension.getRestApi().getInclude());
                task.getOutputResourceDir().set(project.getLayout().getBuildDirectory().dir(compatSpecsDir.toString()));
                task.setSourceResourceDir(
                    yamlCompatTestSourceSet.getResources()
                        .getSrcDirs()
                        .stream()
                        .filter(f -> f.isDirectory() && f.getName().equals("resources"))
                        .findFirst()
                        .orElse(null)
                );
                task.setSkipHasRestTestCheck(true);
                task.setConfigToFileTree(
                    config -> project.fileTree(
                        config.getSingleFile().toPath().resolve(RELATIVE_REST_API_RESOURCES).resolve(RELATIVE_API_PATH)
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
                task.getOutputResourceDir().set(project.getLayout().getBuildDirectory().dir(compatTestsDir.resolve("original").toString()));
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

        // copy both local source set apis and compat apis to a single location to be exported as an artifact
        TaskProvider<Sync> bundleRestCompatApis = project.getTasks().register("bundleRestCompatApis", Sync.class, task -> {
            task.setDestinationDir(project.getLayout().getBuildDirectory().dir("bundledCompatApis").get().getAsFile());
            task.setIncludeEmptyDirs(false);
            task.from(copyCompatYamlSpecTask.flatMap(t -> t.getOutputResourceDir().map(d -> d.dir(RELATIVE_API_PATH.toString()))));
            task.from(yamlCompatTestSourceSet.getProcessResourcesTaskName(), s -> {
                s.include(RELATIVE_API_PATH + "/*");
                s.eachFile(
                    details -> details.setRelativePath(
                        new RelativePath(true, Arrays.stream(details.getRelativePath().getSegments()).skip(2).toArray(String[]::new))
                    )
                );
            });
        });

        // transform the copied tests task
        TaskProvider<RestCompatTestTransformTask> transformCompatTestTask = project.getTasks()
            .register("yamlRestTestV" + COMPATIBLE_VERSION + "CompatTransform", RestCompatTestTransformTask.class, task -> {
                task.getSourceDirectory().set(copyCompatYamlTestTask.flatMap(CopyRestTestsTask::getOutputResourceDir));
                task.getOutputDirectory()
                    .set(project.getLayout().getBuildDirectory().dir(compatTestsDir.resolve("transformed").toString()));
                task.onlyIf(t -> isEnabled(project));
            });

        // Register compat rest resources with source set
        yamlCompatTestSourceSet.getOutput().dir(copyCompatYamlSpecTask.map(CopyRestApiTask::getOutputResourceDir));
        yamlCompatTestSourceSet.getOutput().dir(transformCompatTestTask.map(RestCompatTestTransformTask::getOutputDirectory));

        // Register artifact for transformed compatibility apis and tests
        Configuration compatRestSpecs = project.getConfigurations().create(COMPATIBILITY_APIS_CONFIGURATION);
        Configuration compatRestTests = project.getConfigurations().create(COMPATIBILITY_TESTS_CONFIGURATION);
        project.getArtifacts().add(compatRestSpecs.getName(), bundleRestCompatApis.map(Sync::getDestinationDir));
        project.getArtifacts()
            .add(
                compatRestTests.getName(),
                transformCompatTestTask.flatMap(t -> t.getOutputDirectory().dir(RELATIVE_TEST_PATH.toString()))
            );

        // Grab the original rest resources locations so we can omit them from the compatibility testing classpath down below
        Provider<Directory> originalYamlSpecsDir = project.getTasks()
            .withType(CopyRestApiTask.class)
            .named(RestResourcesPlugin.COPY_REST_API_SPECS_TASK)
            .flatMap(CopyRestApiTask::getOutputResourceDir);
        Provider<Directory> originalYamlTestsDir = project.getTasks()
            .withType(CopyRestTestsTask.class)
            .named(RestResourcesPlugin.COPY_YAML_TESTS_TASK)
            .flatMap(CopyRestTestsTask::getOutputResourceDir);

        String testTaskName = "yamlRestTestV" + COMPATIBLE_VERSION + "CompatTest";

        // setup the test task
        Provider<RestIntegTestTask> yamlRestCompatTestTask = RestTestUtil.registerTestTask(project, yamlCompatTestSourceSet, testTaskName);
        project.getTasks().withType(RestIntegTestTask.class).named(testTaskName).configure(testTask -> {
            // Use test runner and classpath from "normal" yaml source set
            testTask.setTestClassesDirs(
                yamlTestSourceSet.getOutput().getClassesDirs().plus(yamlCompatTestSourceSet.getOutput().getClassesDirs())
            );
            testTask.setClasspath(
                yamlCompatTestSourceSet.getRuntimeClasspath()
                    // remove the "normal" api and tests
                    .minus(project.files(yamlTestSourceSet.getOutput().getResourcesDir()))
                    .minus(project.files(originalYamlSpecsDir))
                    .minus(project.files(originalYamlTestsDir))
            );

            // run compatibility tests after "normal" tests
            testTask.mustRunAfter(project.getTasks().named(InternalYamlRestTestPlugin.SOURCE_SET_NAME));
            testTask.onlyIf(t -> isEnabled(project));
        });

        setupYamlRestTestDependenciesDefaults(project, yamlCompatTestSourceSet);

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
