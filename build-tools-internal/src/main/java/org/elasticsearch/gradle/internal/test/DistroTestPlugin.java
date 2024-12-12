/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.ElasticsearchDistribution.Platform;
import org.elasticsearch.gradle.ElasticsearchDistributionType;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.BwcVersions;
import org.elasticsearch.gradle.internal.InternalDistributionDownloadPlugin;
import org.elasticsearch.gradle.internal.JdkDownloadPlugin;
import org.elasticsearch.gradle.internal.docker.DockerSupportPlugin;
import org.elasticsearch.gradle.internal.docker.DockerSupportService;
import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.specs.Specs;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.gradle.distribution.ElasticsearchDistributionTypes.ARCHIVE;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.ALL_INTERNAL;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.DEB;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.DOCKER;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.DOCKER_CLOUD_ESS;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.DOCKER_IRONBANK;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.DOCKER_WOLFI;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.RPM;
import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * This class defines gradle tasks for testing our various distribution artifacts.
 */
public class DistroTestPlugin implements Plugin<Project> {

    // all distributions used by distro tests. this is temporary until tests are per distribution
    private static final String EXAMPLE_PLUGIN_CONFIGURATION = "examplePlugin";
    private static final String DISTRIBUTION_SYSPROP = "tests.distribution";
    private static final String BWC_DISTRIBUTION_SYSPROP = "tests.bwc-distribution";
    private static final String EXAMPLE_PLUGIN_SYSPROP = "tests.example-plugin";

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(DockerSupportPlugin.class);
        project.getPlugins().apply(InternalDistributionDownloadPlugin.class);
        project.getPlugins().apply(JdkDownloadPlugin.class);
        project.getPluginManager().apply("elasticsearch.java");
        var buildParams = loadBuildParams(project).get();

        Provider<DockerSupportService> dockerSupport = GradleUtils.getBuildService(
            project.getGradle().getSharedServices(),
            DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME
        );

        // TODO: it would be useful to also have the SYSTEM_JAVA_HOME setup in the root project, so that running from GCP only needs
        // a java for gradle to run, and the tests are self sufficient and consistent with the java they use
        NamedDomainObjectContainer<ElasticsearchDistribution> allDistributions = DistributionDownloadPlugin.getContainer(project);
        List<ElasticsearchDistribution> testDistributions = configureDistributions(project);

        Map<ElasticsearchDistributionType, TaskProvider<?>> lifecycleTasks = lifecycleTasks(project, "destructiveDistroTest");
        Map<String, TaskProvider<?>> versionTasks = versionTasks(project, "destructiveDistroUpgradeTest", buildParams.getBwcVersions());
        TaskProvider<Task> destructiveDistroTest = project.getTasks().register("destructiveDistroTest");

        Configuration examplePlugin = configureExamplePlugin(project);

        List<TaskProvider<Test>> windowsTestTasks = new ArrayList<>();
        Map<ElasticsearchDistributionType, List<TaskProvider<Test>>> linuxTestTasks = new HashMap<>();

        for (ElasticsearchDistribution distribution : testDistributions) {
            String taskname = destructiveDistroTestTaskName(distribution);
            ElasticsearchDistributionType type = distribution.getType();
            TaskProvider<Test> destructiveTask = configureTestTask(project, taskname, distribution, t -> {
                t.onlyIf(
                    "Docker is not available",
                    t2 -> distribution.isDocker() == false || dockerSupport.get().getDockerAvailability().isAvailable()
                );
                addDistributionSysprop(t, DISTRIBUTION_SYSPROP, distribution::getFilepath);
                addDistributionSysprop(t, EXAMPLE_PLUGIN_SYSPROP, () -> examplePlugin.getSingleFile().toString());
                t.exclude("**/PackageUpgradeTests.class");
            }, distribution, examplePlugin.getDependencies());

            if (distribution.getPlatform() == Platform.WINDOWS) {
                windowsTestTasks.add(destructiveTask);
            } else {
                linuxTestTasks.computeIfAbsent(type, k -> new ArrayList<>()).add(destructiveTask);
            }
            destructiveDistroTest.configure(t -> t.dependsOn(destructiveTask));
            TaskProvider<?> lifecycleTask = lifecycleTasks.get(type);
            lifecycleTask.configure(t -> t.dependsOn(destructiveTask));

            if ((type == DEB || type == RPM) && distribution.getBundledJdk()) {
                for (Version version : buildParams.getBwcVersions().getIndexCompatible()) {
                    final ElasticsearchDistribution bwcDistro;
                    if (version.equals(Version.fromString(distribution.getVersion()))) {
                        // this is the same as the distribution we are testing
                        bwcDistro = distribution;
                    } else {
                        bwcDistro = createDistro(
                            allDistributions,
                            distribution.getArchitecture(),
                            type,
                            distribution.getPlatform(),
                            distribution.getBundledJdk(),
                            version.toString()
                        );

                    }
                    String upgradeTaskname = destructiveDistroUpgradeTestTaskName(distribution, version.toString());
                    TaskProvider<Test> upgradeTest = configureTestTask(project, upgradeTaskname, distribution, t -> {
                        addDistributionSysprop(t, DISTRIBUTION_SYSPROP, distribution::getFilepath);
                        addDistributionSysprop(t, BWC_DISTRIBUTION_SYSPROP, bwcDistro::getFilepath);
                        t.include("**/PackageUpgradeTests.class");
                    }, distribution, bwcDistro);
                    versionTasks.get(version.toString()).configure(t -> t.dependsOn(upgradeTest));
                }
            }
        }
    }

    private static Map<ElasticsearchDistributionType, TaskProvider<?>> lifecycleTasks(Project project, String taskPrefix) {
        Map<ElasticsearchDistributionType, TaskProvider<?>> lifecyleTasks = new HashMap<>();
        lifecyleTasks.put(DOCKER, project.getTasks().register(taskPrefix + ".docker"));
        lifecyleTasks.put(DOCKER_IRONBANK, project.getTasks().register(taskPrefix + ".docker-ironbank"));
        lifecyleTasks.put(DOCKER_CLOUD_ESS, project.getTasks().register(taskPrefix + ".docker-cloud-ess"));
        lifecyleTasks.put(DOCKER_WOLFI, project.getTasks().register(taskPrefix + ".docker-wolfi"));
        lifecyleTasks.put(ARCHIVE, project.getTasks().register(taskPrefix + ".archives"));
        lifecyleTasks.put(DEB, project.getTasks().register(taskPrefix + ".packages"));
        lifecyleTasks.put(RPM, lifecyleTasks.get(DEB));
        return lifecyleTasks;
    }

    private static Map<String, TaskProvider<?>> versionTasks(Project project, String taskPrefix, BwcVersions bwcVersions) {
        Map<String, TaskProvider<?>> versionTasks = new HashMap<>();

        for (Version version : bwcVersions.getIndexCompatible()) {
            versionTasks.put(version.toString(), project.getTasks().register(taskPrefix + ".v" + version));
        }

        return versionTasks;
    }

    private static Configuration configureExamplePlugin(Project project) {
        Configuration examplePlugin = project.getConfigurations().create(EXAMPLE_PLUGIN_CONFIGURATION);
        examplePlugin.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.ZIP_TYPE);
        DependencyHandler deps = project.getDependencies();
        deps.add(EXAMPLE_PLUGIN_CONFIGURATION, deps.project(Map.of("path", ":plugins:analysis-icu", "configuration", "zip")));
        return examplePlugin;
    }

    private static TaskProvider<Test> configureTestTask(
        Project project,
        String taskname,
        ElasticsearchDistribution distribution,
        Action<? super Test> configure,
        Object... deps
    ) {
        return project.getTasks().register(taskname, Test.class, t -> {
            // Only run tests for the current architecture
            t.onlyIf(t3 -> distribution.getArchitecture() == Architecture.current());
            t.getOutputs().doNotCacheIf("Build cache is disabled for packaging tests", Specs.satisfyAll());
            t.setMaxParallelForks(1);
            SourceSet testSourceSet = project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets().getByName("test");
            t.setClasspath(testSourceSet.getRuntimeClasspath());
            t.setTestClassesDirs(testSourceSet.getOutput().getClassesDirs());
            t.setWorkingDir(project.getProjectDir());
            t.dependsOn(deps);
            configure.execute(t);
        });
    }

    private List<ElasticsearchDistribution> configureDistributions(Project project) {
        NamedDomainObjectContainer<ElasticsearchDistribution> distributions = DistributionDownloadPlugin.getContainer(project);
        List<ElasticsearchDistribution> currentDistros = new ArrayList<>();

        for (Architecture architecture : Architecture.values()) {
            ALL_INTERNAL.forEach(
                type -> currentDistros.add(
                    createDistro(distributions, architecture, type, null, true, VersionProperties.getElasticsearch())
                )
            );
        }

        for (Architecture architecture : Architecture.values()) {
            for (Platform platform : Arrays.asList(Platform.LINUX, Platform.WINDOWS)) {
                currentDistros.add(
                    createDistro(distributions, architecture, ARCHIVE, platform, true, VersionProperties.getElasticsearch())
                );
            }
        }

        return currentDistros;
    }

    private static ElasticsearchDistribution createDistro(
        NamedDomainObjectContainer<ElasticsearchDistribution> distributions,
        Architecture architecture,
        ElasticsearchDistributionType type,
        Platform platform,
        boolean bundledJdk,
        String version
    ) {
        String name = distroId(type, platform, bundledJdk, architecture) + "-" + version;
        boolean isDocker = type.isDocker();
        ElasticsearchDistribution distro = distributions.create(name, d -> {
            d.setArchitecture(architecture);
            d.setType(type);
            if (type == ARCHIVE) {
                d.setPlatform(platform);
            }
            if (isDocker == false) {
                d.setBundledJdk(bundledJdk);
            }
            d.setVersion(version);
            d.setPreferArchive(true);
        });

        // Allow us to gracefully omit building Docker distributions if Docker is not available on the system.
        // In such a case as we can't build the Docker images we'll simply skip the corresponding tests.
        if (isDocker) {
            distro.setFailIfUnavailable(false);
        }

        return distro;
    }

    private static String distroId(ElasticsearchDistributionType type, Platform platform, boolean bundledJdk, Architecture architecture) {
        return "default-"
            + (type == ARCHIVE ? platform + "-" : "")
            + type.getName()
            + (bundledJdk ? "" : "-no-jdk")
            + (architecture == Architecture.X64 ? "" : "-" + architecture.toString().toLowerCase());
    }

    private static String destructiveDistroTestTaskName(ElasticsearchDistribution distro) {
        ElasticsearchDistributionType type = distro.getType();
        return "destructiveDistroTest." + distroId(type, distro.getPlatform(), distro.getBundledJdk(), distro.getArchitecture());
    }

    private static String destructiveDistroUpgradeTestTaskName(ElasticsearchDistribution distro, String bwcVersion) {
        ElasticsearchDistributionType type = distro.getType();
        return "destructiveDistroUpgradeTest.v"
            + bwcVersion
            + "."
            + distroId(type, distro.getPlatform(), distro.getBundledJdk(), distro.getArchitecture());
    }

    private static void addDistributionSysprop(Test task, String sysprop, Supplier<String> valueSupplier) {
        SystemPropertyCommandLineArgumentProvider props = task.getExtensions().getByType(SystemPropertyCommandLineArgumentProvider.class);
        props.systemProperty(sysprop, valueSupplier);
    }
}
