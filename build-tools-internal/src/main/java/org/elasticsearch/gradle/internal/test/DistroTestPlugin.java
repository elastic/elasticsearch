/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.ElasticsearchDistribution.Platform;
import org.elasticsearch.gradle.ElasticsearchDistributionType;
import org.elasticsearch.gradle.internal.Jdk;
import org.elasticsearch.gradle.internal.JdkDownloadPlugin;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.docker.DockerSupportPlugin;
import org.elasticsearch.gradle.internal.docker.DockerSupportService;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.internal.InternalDistributionDownloadPlugin;
import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider;
import org.elasticsearch.gradle.util.GradleUtils;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.internal.vagrant.VagrantBasePlugin;
import org.elasticsearch.gradle.internal.vagrant.VagrantExtension;
import org.elasticsearch.gradle.internal.conventions.GUtils;
import org.gradle.api.Action;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.specs.Specs;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.gradle.distribution.ElasticsearchDistributionTypes.ARCHIVE;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.ALL_INTERNAL;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.DEB;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.DOCKER;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.DOCKER_IRONBANK;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.DOCKER_UBI;
import static org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes.RPM;
import static org.elasticsearch.gradle.internal.vagrant.VagrantMachine.convertLinuxPath;
import static org.elasticsearch.gradle.internal.vagrant.VagrantMachine.convertWindowsPath;

/**
 * This class defines gradle tasks for testing our various distribution artifacts.
 */
public class DistroTestPlugin implements Plugin<Project> {
    private static final String SYSTEM_JDK_VERSION = "11.0.2+9";
    private static final String SYSTEM_JDK_VENDOR = "openjdk";
    private static final String GRADLE_JDK_VERSION = "16.0.1+9";
    private static final String GRADLE_JDK_VENDOR = "adoptopenjdk";

    // all distributions used by distro tests. this is temporary until tests are per distribution
    private static final String EXAMPLE_PLUGIN_CONFIGURATION = "examplePlugin";
    private static final String IN_VM_SYSPROP = "tests.inVM";
    private static final String DISTRIBUTION_SYSPROP = "tests.distribution";
    private static final String BWC_DISTRIBUTION_SYSPROP = "tests.bwc-distribution";
    private static final String EXAMPLE_PLUGIN_SYSPROP = "tests.example-plugin";

    private static final String QUOTA_AWARE_FS_PLUGIN_CONFIGURATION = "quotaAwareFsPlugin";
    private static final String QUOTA_AWARE_FS_PLUGIN_SYSPROP = "tests.quota-aware-fs-plugin";

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(DockerSupportPlugin.class);
        project.getPlugins().apply(InternalDistributionDownloadPlugin.class);
        project.getPlugins().apply(JdkDownloadPlugin.class);
        project.getPluginManager().apply("elasticsearch.build");

        Provider<DockerSupportService> dockerSupport = GradleUtils.getBuildService(
            project.getGradle().getSharedServices(),
            DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME
        );

        // TODO: it would be useful to also have the SYSTEM_JAVA_HOME setup in the root project, so that running from GCP only needs
        // a java for gradle to run, and the tests are self sufficient and consistent with the java they use
        NamedDomainObjectContainer<ElasticsearchDistribution> allDistributions = DistributionDownloadPlugin.getContainer(project);
        List<ElasticsearchDistribution> testDistributions = configureDistributions(project);

        Map<ElasticsearchDistributionType, TaskProvider<?>> lifecycleTasks = lifecycleTasks(project, "destructiveDistroTest");
        Map<String, TaskProvider<?>> versionTasks = versionTasks(project, "destructiveDistroUpgradeTest");
        TaskProvider<Task> destructiveDistroTest = project.getTasks().register("destructiveDistroTest");

        Configuration examplePlugin = configureExamplePlugin(project);
        Configuration quotaAwareFsPlugin = configureQuotaAwareFsPlugin(project);

        List<TaskProvider<Test>> windowsTestTasks = new ArrayList<>();
        Map<ElasticsearchDistributionType, List<TaskProvider<Test>>> linuxTestTasks = new HashMap<>();
        Map<String, List<TaskProvider<Test>>> upgradeTestTasks = new HashMap<>();
        Map<String, TaskProvider<?>> depsTasks = new HashMap<>();

        for (ElasticsearchDistribution distribution : testDistributions) {
            String taskname = destructiveDistroTestTaskName(distribution);
            TaskProvider<?> depsTask = project.getTasks().register(taskname + "#deps");
            // explicitly depend on the archive not on the implicit extracted distribution
            depsTask.configure(t -> t.dependsOn(distribution.getArchiveDependencies(), examplePlugin, quotaAwareFsPlugin));
            depsTasks.put(taskname, depsTask);
            TaskProvider<Test> destructiveTask = configureTestTask(project, taskname, distribution, t -> {
                t.onlyIf(t2 -> distribution.isDocker() == false || dockerSupport.get().getDockerAvailability().isAvailable);
                addDistributionSysprop(t, DISTRIBUTION_SYSPROP, distribution::getFilepath);
                addDistributionSysprop(t, EXAMPLE_PLUGIN_SYSPROP, () -> examplePlugin.getSingleFile().toString());
                addDistributionSysprop(t, QUOTA_AWARE_FS_PLUGIN_SYSPROP, () -> quotaAwareFsPlugin.getSingleFile().toString());
                t.exclude("**/PackageUpgradeTests.class");
            }, depsTask);

            if (distribution.getPlatform() == Platform.WINDOWS) {
                windowsTestTasks.add(destructiveTask);
            } else {
                linuxTestTasks.computeIfAbsent(distribution.getType(), k -> new ArrayList<>()).add(destructiveTask);
            }
            destructiveDistroTest.configure(t -> t.dependsOn(destructiveTask));
            lifecycleTasks.get(distribution.getType()).configure(t -> t.dependsOn(destructiveTask));

            if ((distribution.getType() == DEB || distribution.getType() == RPM) && distribution.getBundledJdk()) {
                for (Version version : BuildParams.getBwcVersions().getIndexCompatible()) {
                    final ElasticsearchDistribution bwcDistro;
                    if (version.equals(Version.fromString(distribution.getVersion()))) {
                        // this is the same as the distribution we are testing
                        bwcDistro = distribution;
                    } else {
                        bwcDistro = createDistro(
                            allDistributions,
                            distribution.getArchitecture(),
                            distribution.getType(),
                            distribution.getPlatform(),
                            distribution.getBundledJdk(),
                            version.toString()
                        );

                    }
                    String upgradeTaskname = destructiveDistroUpgradeTestTaskName(distribution, version.toString());
                    TaskProvider<?> upgradeDepsTask = project.getTasks().register(upgradeTaskname + "#deps");
                    upgradeDepsTask.configure(t -> t.dependsOn(distribution, bwcDistro));
                    depsTasks.put(upgradeTaskname, upgradeDepsTask);
                    TaskProvider<Test> upgradeTest = configureTestTask(project, upgradeTaskname, distribution, t -> {
                        addDistributionSysprop(t, DISTRIBUTION_SYSPROP, distribution::getFilepath);
                        addDistributionSysprop(t, BWC_DISTRIBUTION_SYSPROP, bwcDistro::getFilepath);
                        t.include("**/PackageUpgradeTests.class");
                    }, upgradeDepsTask);
                    versionTasks.get(version.toString()).configure(t -> t.dependsOn(upgradeTest));
                    upgradeTestTasks.computeIfAbsent(version.toString(), k -> new ArrayList<>()).add(upgradeTest);
                }
            }
        }

        // setup jdks used by no-jdk tests, and by gradle executing
        TaskProvider<Copy> linuxGradleJdk = createJdk(project, "gradle", GRADLE_JDK_VENDOR, GRADLE_JDK_VERSION, "linux", "x64");
        TaskProvider<Copy> linuxSystemJdk = createJdk(project, "system", SYSTEM_JDK_VENDOR, SYSTEM_JDK_VERSION, "linux", "x64");
        TaskProvider<Copy> windowsGradleJdk = createJdk(project, "gradle", GRADLE_JDK_VENDOR, GRADLE_JDK_VERSION, "windows", "x64");
        TaskProvider<Copy> windowsSystemJdk = createJdk(project, "system", SYSTEM_JDK_VENDOR, SYSTEM_JDK_VERSION, "windows", "x64");

        project.subprojects(vmProject -> {
            vmProject.getPluginManager().apply(VagrantBasePlugin.class);
            TaskProvider<Copy> gradleJdk = isWindows(vmProject) ? windowsGradleJdk : linuxGradleJdk;
            TaskProvider<Copy> systemJdk = isWindows(vmProject) ? windowsSystemJdk : linuxSystemJdk;
            configureVM(vmProject, gradleJdk, systemJdk);
            List<Object> vmDependencies = Arrays.asList(
                gradleJdk,
                systemJdk,
                project.getConfigurations().getByName("testRuntimeClasspath")
            );

            Map<ElasticsearchDistributionType, TaskProvider<?>> vmLifecyleTasks = lifecycleTasks(vmProject, "distroTest");
            Map<String, TaskProvider<?>> vmVersionTasks = versionTasks(vmProject, "distroUpgradeTest");
            TaskProvider<Task> distroTest = vmProject.getTasks().register("distroTest");

            // windows boxes get windows distributions, and linux boxes get linux distributions
            if (isWindows(vmProject)) {
                configureVMWrapperTasks(
                    vmProject,
                    windowsTestTasks,
                    depsTasks,
                    wrapperTask -> { vmLifecyleTasks.get(ARCHIVE).configure(t -> t.dependsOn(wrapperTask)); },
                    vmDependencies
                );
            } else {
                for (var entry : linuxTestTasks.entrySet()) {
                    ElasticsearchDistributionType type = entry.getKey();
                    TaskProvider<?> vmLifecycleTask = vmLifecyleTasks.get(type);
                    configureVMWrapperTasks(vmProject, entry.getValue(), depsTasks, wrapperTask -> {
                        vmLifecycleTask.configure(t -> t.dependsOn(wrapperTask));

                        // Only VM sub-projects that are specifically opted-in to testing Docker should
                        // have the Docker task added as a dependency. Although we control whether Docker
                        // is installed in the VM via `Vagrantfile` and we could auto-detect its presence
                        // in the VM, the test tasks e.g. `destructiveDistroTest.default-docker` are defined
                        // on the host during Gradle's configuration phase and not in the VM, so
                        // auto-detection doesn't work.
                        //
                        // The shouldTestDocker property could be null, hence we use Boolean.TRUE.equals()
                        boolean shouldExecute = (type.isDocker()) || Boolean.TRUE.equals(vmProject.findProperty("shouldTestDocker"));

                        if (shouldExecute) {
                            distroTest.configure(t -> t.dependsOn(wrapperTask));
                        }
                    }, vmDependencies);
                }

                for (var entry : upgradeTestTasks.entrySet()) {
                    String version = entry.getKey();
                    TaskProvider<?> vmVersionTask = vmVersionTasks.get(version);
                    configureVMWrapperTasks(
                        vmProject,
                        entry.getValue(),
                        depsTasks,
                        wrapperTask -> { vmVersionTask.configure(t -> t.dependsOn(wrapperTask)); },
                        vmDependencies
                    );
                }
            }
        });
    }

    private static Map<ElasticsearchDistributionType, TaskProvider<?>> lifecycleTasks(Project project, String taskPrefix) {
        Map<ElasticsearchDistributionType, TaskProvider<?>> lifecyleTasks = new HashMap<>();
        lifecyleTasks.put(DOCKER, project.getTasks().register(taskPrefix + ".docker"));
        lifecyleTasks.put(DOCKER_UBI, project.getTasks().register(taskPrefix + ".docker-ubi"));
        lifecyleTasks.put(DOCKER_IRONBANK, project.getTasks().register(taskPrefix + ".docker-ironbank"));
        lifecyleTasks.put(ARCHIVE, project.getTasks().register(taskPrefix + ".archives"));
        lifecyleTasks.put(DEB, project.getTasks().register(taskPrefix + ".packages"));
        lifecyleTasks.put(RPM, lifecyleTasks.get(DEB));

        return lifecyleTasks;
    }

    private static Map<String, TaskProvider<?>> versionTasks(Project project, String taskPrefix) {
        Map<String, TaskProvider<?>> versionTasks = new HashMap<>();

        for (Version version : BuildParams.getBwcVersions().getIndexCompatible()) {
            versionTasks.put(version.toString(), project.getTasks().register(taskPrefix + ".v" + version));
        }

        return versionTasks;
    }

    private static TaskProvider<Copy> createJdk(
        Project project,
        String purpose,
        String vendor,
        String version,
        String platform,
        String architecture
    ) {
        Jdk jdk = JdkDownloadPlugin.getContainer(project).create(platform + "-" + purpose);
        jdk.setVendor(vendor);
        jdk.setVersion(version);
        jdk.setPlatform(platform);
        jdk.setArchitecture(architecture);

        String taskname = "copy" + GUtils.capitalize(platform) + GUtils.capitalize(purpose) + "Jdk";
        TaskProvider<Copy> copyTask = project.getTasks().register(taskname, Copy.class);
        copyTask.configure(t -> {
            t.from(jdk);
            t.into(new File(project.getBuildDir(), "jdks/" + platform + "-" + architecture + "-" + vendor + "-" + version));
        });
        return copyTask;
    }

    private static void configureVM(Project project, TaskProvider<Copy> gradleJdkProvider, TaskProvider<Copy> systemJdkProvider) {
        String box = project.getName();

        // setup VM used by these tests
        VagrantExtension vagrant = project.getExtensions().getByType(VagrantExtension.class);
        vagrant.setBox(box);

        vagrant.vmEnv("SYSTEM_JAVA_HOME", convertPath(project, vagrant, systemJdkProvider, "", ""));
        // set java home for gradle to use. package tests will overwrite/remove this for each test case
        vagrant.vmEnv("JAVA_HOME", convertPath(project, vagrant, gradleJdkProvider, "", ""));
        if (System.getenv("JENKINS_URL") != null) {
            Stream.of("JOB_NAME", "JENKINS_URL", "BUILD_NUMBER", "BUILD_URL").forEach(name -> vagrant.vmEnv(name, System.getenv(name)));
        }
        vagrant.setIsWindowsVM(isWindows(project));
    }

    private static Object convertPath(
        Project project,
        VagrantExtension vagrant,
        TaskProvider<Copy> jdkProvider,
        String additionaLinux,
        String additionalWindows
    ) {
        return Util.toStringable(() -> {
            String hostPath = jdkProvider.get().getDestinationDir().toString();
            if (vagrant.isWindowsVM()) {
                return convertWindowsPath(project, hostPath) + additionalWindows;
            } else {
                return convertLinuxPath(project, hostPath) + additionaLinux;
            }
        });
    }

    private static Configuration configureExamplePlugin(Project project) {
        Configuration examplePlugin = project.getConfigurations().create(EXAMPLE_PLUGIN_CONFIGURATION);
        DependencyHandler deps = project.getDependencies();
        Map<String, String> examplePluginProject = Map.of("path", ":example-plugins:custom-settings", "configuration", "zip");
        deps.add(EXAMPLE_PLUGIN_CONFIGURATION, deps.project(examplePluginProject));
        return examplePlugin;
    }

    private static Configuration configureQuotaAwareFsPlugin(Project project) {
        Configuration examplePlugin = project.getConfigurations().create(QUOTA_AWARE_FS_PLUGIN_CONFIGURATION);
        DependencyHandler deps = project.getDependencies();
        Map<String, String> quotaAwareFsPluginProject = Map.of("path", ":x-pack:quota-aware-fs", "configuration", "zip");
        deps.add(QUOTA_AWARE_FS_PLUGIN_CONFIGURATION, deps.project(quotaAwareFsPluginProject));
        return examplePlugin;
    }

    private static void configureVMWrapperTasks(
        Project project,
        List<TaskProvider<Test>> destructiveTasks,
        Map<String, TaskProvider<?>> depsTasks,
        Action<TaskProvider<GradleDistroTestTask>> configure,
        Object... additionalDeps
    ) {
        for (TaskProvider<? extends Task> destructiveTask : destructiveTasks) {
            String destructiveTaskName = destructiveTask.getName();
            String taskname = destructiveTaskName.substring("destructive".length());
            taskname = taskname.substring(0, 1).toLowerCase(Locale.ROOT) + taskname.substring(1);
            TaskProvider<GradleDistroTestTask> vmTask = project.getTasks().register(taskname, GradleDistroTestTask.class, t -> {
                t.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
                t.setDescription("Runs " + destructiveTaskName.split("\\.", 2)[1] + " tests within vagrant");
                t.setTaskName(destructiveTaskName);
                t.extraArg("-D'" + IN_VM_SYSPROP + "'");
                t.dependsOn(depsTasks.get(destructiveTaskName));
                t.dependsOn(additionalDeps);
            });
            configure.execute(vmTask);
        }
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
            t.setWorkingDir(project.getProjectDir());
            if (System.getProperty(IN_VM_SYSPROP) == null) {
                t.dependsOn(deps);
            }
            configure.execute(t);
        });
    }

    private List<ElasticsearchDistribution> configureDistributions(Project project) {
        NamedDomainObjectContainer<ElasticsearchDistribution> distributions = DistributionDownloadPlugin.getContainer(project);
        List<ElasticsearchDistribution> currentDistros = new ArrayList<>();

        for (Architecture architecture : Architecture.values()) {
            ALL_INTERNAL.stream().forEach(type -> {
                for (boolean bundledJdk : Arrays.asList(true, false)) {
                    if (bundledJdk == false) {
                        // We'll never publish an ARM (aarch64) build without a bundled JDK.
                        if (architecture == Architecture.AARCH64) {
                            continue;
                        }
                        // All our Docker images include a bundled JDK so it doesn't make sense to test without one.
                        if (type.isDocker()) {
                            continue;
                        }
                    }
                    currentDistros.add(
                        createDistro(distributions, architecture, type, null, bundledJdk, VersionProperties.getElasticsearch())
                    );
                }
            });
        }

        for (Architecture architecture : Architecture.values()) {
            for (Platform platform : Arrays.asList(Platform.LINUX, Platform.WINDOWS)) {
                for (boolean bundledJdk : Arrays.asList(true, false)) {
                    if (bundledJdk == false && architecture != Architecture.X64) {
                        // We will never publish distributions for non-x86 (amd64) platforms
                        // without a bundled JDK
                        continue;
                    }

                    currentDistros.add(
                        createDistro(distributions, architecture, ARCHIVE, platform, bundledJdk, VersionProperties.getElasticsearch())
                    );
                }
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
        });

        // Allow us to gracefully omit building Docker distributions if Docker is not available on the system.
        // In such a case as we can't build the Docker images we'll simply skip the corresponding tests.
        if (isDocker) {
            distro.setFailIfUnavailable(false);
        }

        return distro;
    }

    // return true if the project is for a windows VM, false otherwise
    private static boolean isWindows(Project project) {
        return project.getName().contains("windows");
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
