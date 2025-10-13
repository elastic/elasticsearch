/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest.compat.compat;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.internal.ElasticsearchJavaBasePlugin;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.test.rest.CopyRestApiTask;
import org.elasticsearch.gradle.internal.test.rest.CopyRestTestsTask;
import org.elasticsearch.gradle.internal.test.rest.LegacyYamlRestTestPlugin;
import org.elasticsearch.gradle.internal.test.rest.RestResourcesExtension;
import org.elasticsearch.gradle.internal.test.rest.RestResourcesPlugin;
import org.elasticsearch.gradle.test.YamlRestTestPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.file.Directory;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RelativePath;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.language.jvm.tasks.ProcessResources;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.test.rest.RestTestUtil.setupYamlRestTestDependenciesDefaults;
import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * Apply this plugin to run the YAML based REST tests from a prior major version against this version's cluster.
 */
public abstract class AbstractYamlRestCompatTestPlugin implements Plugin<Project> {
    public static final String BWC_MINOR_CONFIG_NAME = "bwcMinor";
    private static final String REST_COMPAT_CHECK_TASK_NAME = "checkRestCompat";
    private static final String COMPATIBILITY_APIS_CONFIGURATION = "restCompatSpecs";
    private static final String COMPATIBILITY_TESTS_CONFIGURATION = "restCompatTests";
    private static final Path RELATIVE_API_PATH = Path.of("rest-api-spec/api");
    private static final Path RELATIVE_TEST_PATH = Path.of("rest-api-spec/test");
    private static final Path RELATIVE_REST_API_RESOURCES = Path.of("rest-api-spec/src/main/resources");
    private static final Path RELATIVE_REST_CORE = Path.of("rest-api-spec");
    private static final Path RELATIVE_REST_XPACK = Path.of("x-pack/plugin");
    private static final Path RELATIVE_REST_PROJECT_RESOURCES = Path.of("src/yamlRestTest/resources");
    private static final String SOURCE_SET_NAME = "yamlRestCompatTest";
    private ProjectLayout projectLayout;
    private FileOperations fileOperations;

    @Inject
    public AbstractYamlRestCompatTestPlugin(ProjectLayout projectLayout, FileOperations fileOperations) {
        this.projectLayout = projectLayout;
        this.fileOperations = fileOperations;
    }

    @Override
    public void apply(Project project) {
        project.getRootProject().getRootProject().getPlugins().apply(GlobalBuildInfoPlugin.class);
        var buildParams = loadBuildParams(project).get();

        final Path compatRestResourcesDir = Path.of("restResources").resolve("compat");
        final Path compatSpecsDir = compatRestResourcesDir.resolve("yamlSpecs");
        final Path compatTestsDir = compatRestResourcesDir.resolve("yamlTests");
        project.getPluginManager().apply(getBasePlugin());
        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);
        project.getPluginManager().apply(RestResourcesPlugin.class);

        RestResourcesExtension extension = project.getExtensions().getByType(RestResourcesExtension.class);

        // create source set
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet yamlCompatTestSourceSet = sourceSets.create(SOURCE_SET_NAME);
        SourceSet yamlTestSourceSet = sourceSets.getByName(YamlRestTestPlugin.YAML_REST_TEST);
        GradleUtils.extendSourceSet(project, YamlRestTestPlugin.YAML_REST_TEST, SOURCE_SET_NAME);

        // determine the previous rest compatibility version and BWC project path
        int currentMajor = buildParams.getBwcVersions().getCurrentVersion().getMajor();
        Version lastMinor = buildParams.getBwcVersions()
            .getUnreleased()
            .stream()
            .filter(v -> v.getMajor() == currentMajor - 1)
            .min(Comparator.reverseOrder())
            .get();
        String lastMinorProjectPath = buildParams.getBwcVersions().unreleasedInfo(lastMinor).gradleProjectPath();

        // copy compatible rest specs
        Configuration bwcMinorConfig = project.getConfigurations().create(BWC_MINOR_CONFIG_NAME);
        Dependency bwcMinor = project.getDependencies().project(Map.of("path", lastMinorProjectPath, "configuration", "checkout"));
        project.getDependencies().add(bwcMinorConfig.getName(), bwcMinor);

        String projectPath = project.getPath();
        ExtraPropertiesExtension extraProperties = project.getExtensions().getExtraProperties();
        Provider<CopyRestApiTask> copyCompatYamlSpecTask = project.getTasks()
            .register("copyRestCompatApiTask", CopyRestApiTask.class, task -> {
                task.dependsOn(bwcMinorConfig);
                task.setConfig(bwcMinorConfig);
                task.setAdditionalConfig(bwcMinorConfig);
                task.getInclude().set(extension.getRestApi().getInclude());
                task.getOutputResourceDir().set(projectLayout.getBuildDirectory().dir(compatSpecsDir.toString()));
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
                    config -> fileOperations.fileTree(
                        config.getSingleFile().toPath().resolve(RELATIVE_REST_API_RESOURCES).resolve(RELATIVE_API_PATH)
                    )
                );
                task.setAdditionalConfigToFileTree(
                    config -> fileOperations.fileTree(
                        getCompatProjectPath(projectPath, config.getSingleFile().toPath()).resolve(RELATIVE_REST_PROJECT_RESOURCES)
                            .resolve(RELATIVE_API_PATH)
                    )
                );
                onlyIfBwcEnabled(task, extraProperties);
                // task.onlyIf(t -> isEnabled(extraProperties));
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
                task.getOutputResourceDir().set(projectLayout.getBuildDirectory().dir(compatTestsDir.resolve("original").toString()));
                task.setCoreConfigToFileTree(
                    config -> fileOperations.fileTree(
                        config.getSingleFile()
                            .toPath()
                            .resolve(RELATIVE_REST_CORE)
                            .resolve(RELATIVE_REST_PROJECT_RESOURCES)
                            .resolve(RELATIVE_TEST_PATH)
                    )
                );
                task.setXpackConfigToFileTree(
                    config -> fileOperations.fileTree(
                        config.getSingleFile()
                            .toPath()
                            .resolve(RELATIVE_REST_XPACK)
                            .resolve(RELATIVE_REST_PROJECT_RESOURCES)
                            .resolve(RELATIVE_TEST_PATH)
                    )
                );
                task.setAdditionalConfigToFileTree(
                    config -> fileOperations.fileTree(
                        getCompatProjectPath(projectPath, config.getSingleFile().toPath()).resolve(RELATIVE_REST_PROJECT_RESOURCES)
                            .resolve(RELATIVE_TEST_PATH)
                    )
                );
                task.dependsOn(copyCompatYamlSpecTask);
                onlyIfBwcEnabled(task, extraProperties);
            });

        // copy both local source set apis and compat apis to a single location to be exported as an artifact
        TaskProvider<Sync> bundleRestCompatApis = project.getTasks().register("bundleRestCompatApis", Sync.class, task -> {
            task.setDestinationDir(projectLayout.getBuildDirectory().dir("bundledCompatApis").get().getAsFile());
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
            .register("yamlRestCompatTestTransform", RestCompatTestTransformTask.class, task -> {
                task.getSourceDirectory().set(copyCompatYamlTestTask.flatMap(CopyRestTestsTask::getOutputResourceDir));
                task.getOutputDirectory()
                    .set(project.getLayout().getBuildDirectory().dir(compatTestsDir.resolve("transformed").toString()));
                onlyIfBwcEnabled(task, extraProperties);
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

        // ensure we include other non rest spec related test resources
        project.getTasks()
            .withType(ProcessResources.class)
            .named(yamlCompatTestSourceSet.getProcessResourcesTaskName())
            .configure(processResources -> {
                processResources.from(
                    sourceSets.getByName(YamlRestTestPlugin.YAML_REST_TEST).getResources(),
                    spec -> { spec.exclude("rest-api-spec/**"); }
                );
            });

        // setup the test task
        TaskProvider<? extends Test> yamlRestCompatTestTask = registerTestTask(project, yamlCompatTestSourceSet);
        yamlRestCompatTestTask.configure(testTask -> {
            testTask.systemProperty("tests.restCompat", true);
            // Use test runner and classpath from "normal" yaml source set
            FileCollection outputFileCollection = yamlCompatTestSourceSet.getOutput();
            testTask.setTestClassesDirs(
                yamlTestSourceSet.getOutput().getClassesDirs().plus(yamlCompatTestSourceSet.getOutput().getClassesDirs())
            );
            testTask.onlyIf("Compatibility tests are available", t -> outputFileCollection.isEmpty() == false);
            testTask.setClasspath(
                yamlCompatTestSourceSet.getRuntimeClasspath()
                    // remove the "normal" api and tests
                    .minus(project.files(yamlTestSourceSet.getOutput().getResourcesDir()))
                    .minus(project.files(originalYamlSpecsDir))
                    .minus(project.files(originalYamlTestsDir))
            );

            // run compatibility tests after "normal" tests
            testTask.mustRunAfter(project.getTasks().named(LegacyYamlRestTestPlugin.SOURCE_SET_NAME));
            onlyIfBwcEnabled(testTask, extraProperties);
        });

        setupYamlRestTestDependenciesDefaults(project, yamlCompatTestSourceSet, true);

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

    public abstract TaskProvider<? extends Test> registerTestTask(Project project, SourceSet sourceSet);

    public abstract Class<? extends Plugin<Project>> getBasePlugin();

    private void onlyIfBwcEnabled(Task task, ExtraPropertiesExtension extraProperties) {
        task.onlyIf("BWC tests disabled", t -> isEnabled(extraProperties));
    }

    private boolean isEnabled(ExtraPropertiesExtension extraProperties) {
        Object bwcEnabled = extraProperties.getProperties().get("bwc_tests_enabled");
        return bwcEnabled == null || (Boolean) bwcEnabled;
    }

    // TODO: implement custom extension that allows us move around of the projects between major versions and still find them
    private Path getCompatProjectPath(String projectPath, Path checkoutDir) {
        return checkoutDir.resolve(projectPath.replaceFirst(":", "").replace(":", File.separator));
    }
}
