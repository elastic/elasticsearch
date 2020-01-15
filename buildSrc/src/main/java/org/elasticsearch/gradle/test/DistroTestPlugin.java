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

package org.elasticsearch.gradle.test;

import org.elasticsearch.gradle.BwcVersions;
import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.ElasticsearchDistribution.Flavor;
import org.elasticsearch.gradle.ElasticsearchDistribution.Platform;
import org.elasticsearch.gradle.ElasticsearchDistribution.Type;
import org.elasticsearch.gradle.Jdk;
import org.elasticsearch.gradle.JdkDownloadPlugin;
import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.vagrant.BatsProgressLogger;
import org.elasticsearch.gradle.vagrant.VagrantBasePlugin;
import org.elasticsearch.gradle.vagrant.VagrantExtension;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.Directory;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskInputs;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.gradle.vagrant.VagrantMachine.convertLinuxPath;
import static org.elasticsearch.gradle.vagrant.VagrantMachine.convertWindowsPath;

public class DistroTestPlugin implements Plugin<Project> {
    private static final Logger logger = Logging.getLogger(DistroTestPlugin.class);

    private static final String SYSTEM_JDK_VERSION = "11.0.2+9";
    private static final String SYSTEM_JDK_VENDOR = "openjdk";
    private static final String GRADLE_JDK_VERSION = "13.0.1+9@cec27d702aa74d5a8630c65ae61e4305";
    private static final String GRADLE_JDK_VENDOR = "openjdk";

    // all distributions used by distro tests. this is temporary until tests are per distribution
    private static final String DISTRIBUTIONS_CONFIGURATION = "distributions";
    private static final String UPGRADE_CONFIGURATION = "upgradeDistributions";
    private static final String PLUGINS_CONFIGURATION = "packagingPlugins";
    private static final String COPY_DISTRIBUTIONS_TASK = "copyDistributions";
    private static final String COPY_UPGRADE_TASK = "copyUpgradePackages";
    private static final String COPY_PLUGINS_TASK = "copyPlugins";
    private static final String IN_VM_SYSPROP = "tests.inVM";
    private static final String DISTRIBUTION_SYSPROP = "tests.distribution";

    @Override
    public void apply(Project project) {
        final boolean runDockerTests = shouldRunDockerTests(project);

        project.getPluginManager().apply(DistributionDownloadPlugin.class);
        project.getPluginManager().apply("elasticsearch.build");

        // TODO: it would be useful to also have the SYSTEM_JAVA_HOME setup in the root project, so that running from GCP only needs
        // a java for gradle to run, and the tests are self sufficient and consistent with the java they use

        Version upgradeVersion = getUpgradeVersion(project);
        Provider<Directory> distributionsDir = project.getLayout().getBuildDirectory().dir("packaging/distributions");
        Provider<Directory> upgradeDir = project.getLayout().getBuildDirectory().dir("packaging/upgrade");
        Provider<Directory> pluginsDir = project.getLayout().getBuildDirectory().dir("packaging/plugins");

        List<ElasticsearchDistribution> distributions = configureDistributions(project, upgradeVersion, runDockerTests);
        TaskProvider<Copy> copyDistributionsTask = configureCopyDistributionsTask(project, distributionsDir);
        TaskProvider<Copy> copyUpgradeTask = configureCopyUpgradeTask(project, upgradeVersion, upgradeDir);
        TaskProvider<Copy> copyPluginsTask = configureCopyPluginsTask(project, pluginsDir);

        TaskProvider<Task> destructiveDistroTest = project.getTasks().register("destructiveDistroTest");
        for (ElasticsearchDistribution distribution : distributions) {
            if (distribution.getType() != Type.DOCKER || runDockerTests == true) {
                TaskProvider<?> destructiveTask = configureDistroTest(project, distribution);
                destructiveDistroTest.configure(t -> t.dependsOn(destructiveTask));
            }
        }
        Map<String, TaskProvider<?>> batsTests = new HashMap<>();
        batsTests.put("bats oss", configureBatsTest(project, "oss", distributionsDir, copyDistributionsTask));
        configureBatsTest(project, "plugins", distributionsDir, copyDistributionsTask, copyPluginsTask).configure(
            t -> t.setPluginsDir(pluginsDir)
        );
        configureBatsTest(project, "upgrade", distributionsDir, copyDistributionsTask, copyUpgradeTask).configure(
            t -> t.setUpgradeDir(upgradeDir)
        );

        project.subprojects(vmProject -> {
            vmProject.getPluginManager().apply(VagrantBasePlugin.class);
            vmProject.getPluginManager().apply(JdkDownloadPlugin.class);
            List<Object> vmDependencies = new ArrayList<>(configureVM(vmProject));
            vmDependencies.add(project.getConfigurations().getByName("testRuntimeClasspath"));

            TaskProvider<Task> distroTest = vmProject.getTasks().register("distroTest");
            for (ElasticsearchDistribution distribution : distributions) {
                String destructiveTaskName = destructiveDistroTestTaskName(distribution);
                Platform platform = distribution.getPlatform();
                // this condition ensures windows boxes get windows distributions, and linux boxes get linux distributions
                if (isWindows(vmProject) == (platform == Platform.WINDOWS)) {
                    TaskProvider<GradleDistroTestTask> vmTask = configureVMWrapperTask(
                        vmProject,
                        distribution.getName() + " distribution",
                        destructiveTaskName,
                        vmDependencies
                    );
                    vmTask.configure(t -> t.dependsOn(distribution));

                    distroTest.configure(t -> {
                        // Only VM sub-projects that are specifically opted-in to testing Docker should
                        // have the Docker task added as a dependency. Although we control whether Docker
                        // is installed in the VM via `Vagrantfile` and we could auto-detect its presence
                        // in the VM, the test tasks e.g. `destructiveDistroTest.default-docker` are defined
                        // on the host during Gradle's configuration phase and not in the VM, so
                        // auto-detection doesn't work.
                        //
                        // The shouldTestDocker property could be null, hence we use Boolean.TRUE.equals()
                        boolean shouldExecute = distribution.getType() != Type.DOCKER
                            || Boolean.TRUE.equals(vmProject.findProperty("shouldTestDocker")) == true;

                        if (shouldExecute) {
                            t.dependsOn(vmTask);
                        }
                    });
                }
            }

            batsTests.forEach((desc, task) -> {
                configureVMWrapperTask(vmProject, desc, task.getName(), vmDependencies).configure(t -> {
                    t.setProgressHandler(new BatsProgressLogger(project.getLogger()));
                    t.onlyIf(spec -> isWindows(vmProject) == false); // bats doesn't run on windows
                    t.dependsOn(copyDistributionsTask);
                });
            });
        });
    }

    private static Jdk createJdk(
        NamedDomainObjectContainer<Jdk> jdksContainer,
        String name,
        String vendor,
        String version,
        String platform
    ) {
        Jdk jdk = jdksContainer.create(name);
        jdk.setVendor(vendor);
        jdk.setVersion(version);
        jdk.setPlatform(platform);
        return jdk;
    }

    private static Version getUpgradeVersion(Project project) {
        String upgradeFromVersionRaw = System.getProperty("tests.packaging.upgradeVersion");
        if (upgradeFromVersionRaw != null) {
            return Version.fromString(upgradeFromVersionRaw);
        }

        // was not passed in, so randomly choose one from bwc versions
        ExtraPropertiesExtension extraProperties = project.getExtensions().getByType(ExtraPropertiesExtension.class);

        if ((boolean) extraProperties.get("bwc_tests_enabled") == false) {
            // Upgrade tests will go from current to current when the BWC tests are disabled to skip real BWC tests
            return Version.fromString(project.getVersion().toString());
        }

        String firstPartOfSeed = BuildParams.getTestSeed().split(":")[0];
        final long seed = Long.parseUnsignedLong(firstPartOfSeed, 16);
        BwcVersions bwcVersions = (BwcVersions) extraProperties.get("bwcVersions");
        final List<Version> indexCompatVersions = bwcVersions.getIndexCompatible();
        return indexCompatVersions.get(new Random(seed).nextInt(indexCompatVersions.size()));
    }

    private static List<Object> configureVM(Project project) {
        String box = project.getName();

        // setup jdks used by the distro tests, and by gradle executing

        NamedDomainObjectContainer<Jdk> jdksContainer = JdkDownloadPlugin.getContainer(project);
        String platform = box.contains("windows") ? "windows" : "linux";
        Jdk systemJdk = createJdk(jdksContainer, "system", SYSTEM_JDK_VENDOR, SYSTEM_JDK_VERSION, platform);
        Jdk gradleJdk = createJdk(jdksContainer, "gradle", GRADLE_JDK_VENDOR, GRADLE_JDK_VERSION, platform);

        // setup VM used by these tests
        VagrantExtension vagrant = project.getExtensions().getByType(VagrantExtension.class);
        vagrant.setBox(box);
        vagrant.vmEnv("SYSTEM_JAVA_HOME", convertPath(project, vagrant, systemJdk, "", ""));
        vagrant.vmEnv("PATH", convertPath(project, vagrant, gradleJdk, "/bin:$PATH", "\\bin;$Env:PATH"));
        // pass these along to get correct build scans
        if (System.getenv("JENKINS_URL") != null) {
            Stream.of("JOB_NAME", "JENKINS_URL", "BUILD_NUMBER", "BUILD_URL").forEach(name -> vagrant.vmEnv(name, System.getenv(name)));
        }
        vagrant.setIsWindowsVM(isWindows(project));

        return Arrays.asList(systemJdk, gradleJdk);
    }

    private static Object convertPath(Project project, VagrantExtension vagrant, Jdk jdk, String additionaLinux, String additionalWindows) {
        return new Object() {
            @Override
            public String toString() {
                if (vagrant.isWindowsVM()) {
                    return convertWindowsPath(project, jdk.getPath()) + additionalWindows;
                }
                return convertLinuxPath(project, jdk.getPath()) + additionaLinux;
            }
        };
    }

    private static TaskProvider<Copy> configureCopyDistributionsTask(Project project, Provider<Directory> distributionsDir) {

        // temporary, until we have tasks per distribution
        return project.getTasks().register(COPY_DISTRIBUTIONS_TASK, Copy.class, t -> {
            t.into(distributionsDir);
            t.from(project.getConfigurations().getByName(DISTRIBUTIONS_CONFIGURATION));

            Path distributionsPath = distributionsDir.get().getAsFile().toPath();
            TaskInputs inputs = t.getInputs();
            inputs.property("version", VersionProperties.getElasticsearch());
            t.doLast(action -> {
                try {
                    Files.writeString(distributionsPath.resolve("version"), VersionProperties.getElasticsearch());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        });
    }

    private static TaskProvider<Copy> configureCopyUpgradeTask(Project project, Version upgradeVersion, Provider<Directory> upgradeDir) {
        // temporary, until we have tasks per distribution
        return project.getTasks().register(COPY_UPGRADE_TASK, Copy.class, t -> {
            t.into(upgradeDir);
            t.from(project.getConfigurations().getByName(UPGRADE_CONFIGURATION));

            Path upgradePath = upgradeDir.get().getAsFile().toPath();

            // write bwc version, and append -SNAPSHOT if it is an unreleased version
            ExtraPropertiesExtension extraProperties = project.getExtensions().getByType(ExtraPropertiesExtension.class);
            BwcVersions bwcVersions = (BwcVersions) extraProperties.get("bwcVersions");
            final String upgradeFromVersion;
            if (bwcVersions.unreleasedInfo(upgradeVersion) != null) {
                upgradeFromVersion = upgradeVersion.toString() + "-SNAPSHOT";
            } else {
                upgradeFromVersion = upgradeVersion.toString();
            }
            TaskInputs inputs = t.getInputs();
            inputs.property("upgrade_from_version", upgradeFromVersion);
            // TODO: this is serializable, need to think how to represent this as an input
            // inputs.property("bwc_versions", bwcVersions);
            t.doLast(action -> {
                try {
                    Files.writeString(upgradePath.resolve("upgrade_from_version"), upgradeFromVersion);
                    // this is always true, but bats tests rely on it. It is just temporary until bats is removed.
                    Files.writeString(upgradePath.resolve("upgrade_is_oss"), "");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        });
    }

    private static TaskProvider<Copy> configureCopyPluginsTask(Project project, Provider<Directory> pluginsDir) {
        Configuration pluginsConfiguration = project.getConfigurations().create(PLUGINS_CONFIGURATION);

        // temporary, until we have tasks per distribution
        return project.getTasks().register(COPY_PLUGINS_TASK, Copy.class, t -> {
            t.into(pluginsDir);
            t.from(pluginsConfiguration);
        });
    }

    private static TaskProvider<GradleDistroTestTask> configureVMWrapperTask(
        Project project,
        String type,
        String destructiveTaskPath,
        List<Object> dependsOn
    ) {
        int taskNameStart = destructiveTaskPath.lastIndexOf(':') + "destructive".length() + 1;
        String taskname = destructiveTaskPath.substring(taskNameStart);
        taskname = taskname.substring(0, 1).toLowerCase(Locale.ROOT) + taskname.substring(1);
        return project.getTasks().register(taskname, GradleDistroTestTask.class, t -> {
            t.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            t.setDescription("Runs " + type + " tests within vagrant");
            t.setTaskName(destructiveTaskPath);
            t.extraArg("-D'" + IN_VM_SYSPROP + "'");
            t.dependsOn(dependsOn);
        });
    }

    private static TaskProvider<?> configureDistroTest(Project project, ElasticsearchDistribution distribution) {
        return project.getTasks().register(destructiveDistroTestTaskName(distribution), Test.class, t -> {
            t.setMaxParallelForks(1);
            t.setWorkingDir(project.getProjectDir());
            t.systemProperty(DISTRIBUTION_SYSPROP, distribution.toString());
            if (System.getProperty(IN_VM_SYSPROP) == null) {
                t.dependsOn(distribution);
            }
        });
    }

    private static TaskProvider<BatsTestTask> configureBatsTest(
        Project project,
        String type,
        Provider<Directory> distributionsDir,
        Object... deps
    ) {
        return project.getTasks().register("destructiveBatsTest." + type, BatsTestTask.class, t -> {
            Directory batsDir = project.getLayout().getProjectDirectory().dir("bats");
            t.setTestsDir(batsDir.dir(type));
            t.setUtilsDir(batsDir.dir("utils"));
            t.setDistributionsDir(distributionsDir);
            t.setPackageName("elasticsearch" + (type.equals("oss") ? "-oss" : ""));
            if (System.getProperty(IN_VM_SYSPROP) == null) {
                t.dependsOn(deps);
            }
        });
    }

    private List<ElasticsearchDistribution> configureDistributions(Project project, Version upgradeVersion, boolean runDockerTests) {
        NamedDomainObjectContainer<ElasticsearchDistribution> distributions = DistributionDownloadPlugin.getContainer(project);
        List<ElasticsearchDistribution> currentDistros = new ArrayList<>();
        List<ElasticsearchDistribution> upgradeDistros = new ArrayList<>();

        for (Type type : List.of(Type.DEB, Type.RPM, Type.DOCKER)) {
            for (Flavor flavor : Flavor.values()) {
                for (boolean bundledJdk : Arrays.asList(true, false)) {
                    // All our Docker images include a bundled JDK so it doesn't make sense to test without one
                    boolean skip = type == Type.DOCKER && (runDockerTests == false || bundledJdk == false);

                    if (skip == false) {
                        addDistro(distributions, type, null, flavor, bundledJdk, VersionProperties.getElasticsearch(), currentDistros);
                    }
                }
            }

            // We don't configure distributions for prior versions for Docker. This is because doing
            // so prompts Gradle to try and resolve the Docker dependencies, which doesn't work as
            // they can't be downloaded via Ivy (configured in DistributionDownloadPlugin). Since we
            // need these for the BATS upgrade tests, and those tests only cover .rpm and .deb, it's
            // OK to omit creating such distributions in the first place. We may need to revisit
            // this in the future, so allow upgrade testing using Docker containers.
            if (type != Type.DOCKER) {
                // upgrade version is always bundled jdk
                // NOTE: this is mimicking the old VagrantTestPlugin upgrade behavior. It will eventually be replaced
                // witha dedicated upgrade test from every bwc version like other bwc tests
                addDistro(distributions, type, null, Flavor.DEFAULT, true, upgradeVersion.toString(), upgradeDistros);
                if (upgradeVersion.onOrAfter("6.3.0")) {
                    addDistro(distributions, type, null, Flavor.OSS, true, upgradeVersion.toString(), upgradeDistros);
                }
            }
        }

        for (Platform platform : Arrays.asList(Platform.LINUX, Platform.WINDOWS)) {
            for (Flavor flavor : Flavor.values()) {
                for (boolean bundledJdk : Arrays.asList(true, false)) {
                    addDistro(
                        distributions,
                        Type.ARCHIVE,
                        platform,
                        flavor,
                        bundledJdk,
                        VersionProperties.getElasticsearch(),
                        currentDistros
                    );
                }
            }
        }

        // temporary until distro tests have one test per distro
        Configuration packagingConfig = project.getConfigurations().create(DISTRIBUTIONS_CONFIGURATION);
        List<Configuration> distroConfigs = currentDistros.stream()
            .filter(d -> d.getType() != Type.DOCKER)
            .map(ElasticsearchDistribution::getConfiguration)
            .collect(Collectors.toList());
        packagingConfig.setExtendsFrom(distroConfigs);

        Configuration packagingUpgradeConfig = project.getConfigurations().create(UPGRADE_CONFIGURATION);
        List<Configuration> distroUpgradeConfigs = upgradeDistros.stream()
            .map(ElasticsearchDistribution::getConfiguration)
            .collect(Collectors.toList());
        packagingUpgradeConfig.setExtendsFrom(distroUpgradeConfigs);

        return currentDistros;
    }

    private static void addDistro(
        NamedDomainObjectContainer<ElasticsearchDistribution> distributions,
        Type type,
        Platform platform,
        Flavor flavor,
        boolean bundledJdk,
        String version,
        List<ElasticsearchDistribution> container
    ) {

        String name = distroId(type, platform, flavor, bundledJdk) + "-" + version;
        if (distributions.findByName(name) != null) {
            return;
        }
        ElasticsearchDistribution distro = distributions.create(name, d -> {
            d.setFlavor(flavor);
            d.setType(type);
            if (type == Type.ARCHIVE) {
                d.setPlatform(platform);
            }
            d.setBundledJdk(bundledJdk);
            d.setVersion(version);
        });
        container.add(distro);
    }

    // return true if the project is for a windows VM, false otherwise
    private static boolean isWindows(Project project) {
        return project.getName().contains("windows");
    }

    private static String distroId(Type type, Platform platform, Flavor flavor, boolean bundledJdk) {
        return flavor + "-" + (type == Type.ARCHIVE ? platform + "-" : "") + type + (bundledJdk ? "" : "-no-jdk");
    }

    private static String destructiveDistroTestTaskName(ElasticsearchDistribution distro) {
        Type type = distro.getType();
        return "destructiveDistroTest." + distroId(type, distro.getPlatform(), distro.getFlavor(), distro.getBundledJdk());
    }

    static Map<String, String> parseOsRelease(final List<String> osReleaseLines) {
        final Map<String, String> values = new HashMap<>();

        osReleaseLines.stream().map(String::trim).filter(line -> (line.isEmpty() || line.startsWith("#")) == false).forEach(line -> {
            final String[] parts = line.split("=", 2);
            final String key = parts[0];
            // remove optional leading and trailing quotes and whitespace
            final String value = parts[1].replaceAll("^['\"]?\\s*", "").replaceAll("\\s*['\"]?$", "");

            values.put(key, value);
        });

        return values;
    }

    static String deriveId(final Map<String, String> osRelease) {
        return osRelease.get("ID") + "-" + osRelease.get("VERSION_ID");
    }

    private static List<String> getLinuxExclusionList(Project project) {
        final String exclusionsFilename = "dockerOnLinuxExclusions";
        final Path exclusionsPath = project.getRootDir().toPath().resolve(Path.of(".ci", exclusionsFilename));

        try {
            return Files.readAllLines(exclusionsPath)
                .stream()
                .map(String::trim)
                .filter(line -> (line.isEmpty() || line.startsWith("#")) == false)
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new GradleException("Failed to read .ci/" + exclusionsFilename, e);
        }
    }

    /**
     * The {@link DistroTestPlugin} generates a number of test tasks, some
     * of which are Docker packaging tests. When running on the host OS or in CI
     * i.e. not in a Vagrant VM, only certain operating systems are supported. This
     * method determines whether the Docker tests should be run on the host
     * OS. Essentially, unless an OS and version is specifically excluded, we expect
     * to be able to run Docker and test the Docker images.
     */
    private static boolean shouldRunDockerTests(Project project) {
        switch (OS.current()) {
            case WINDOWS:
                // Not yet supported.
                return false;

            case MAC:
                // Assume that Docker for Mac is installed, since Docker is part of the dev workflow.
                return true;

            case LINUX:
                // We don't attempt to check the current flavor and version of Linux unless we're
                // running in CI, because we don't want to stop people running the Docker tests in
                // their own environments if they really want to.
                if (BuildParams.isCi() == false) {
                    return true;
                }

                // Only some hosts in CI are configured with Docker. We attempt to work out the OS
                // and version, so that we know whether to expect to find Docker. We don't attempt
                // to probe for whether Docker is available, because that doesn't tell us whether
                // Docker is unavailable when it should be.
                final Path osRelease = Paths.get("/etc/os-release");

                if (Files.exists(osRelease)) {
                    Map<String, String> values;

                    try {
                        final List<String> osReleaseLines = Files.readAllLines(osRelease);
                        values = parseOsRelease(osReleaseLines);
                    } catch (IOException e) {
                        throw new GradleException("Failed to read /etc/os-release", e);
                    }

                    final String id = deriveId(values);

                    final boolean shouldExclude = getLinuxExclusionList(project).contains(id);

                    logger.warn("Linux OS id [" + id + "] is " + (shouldExclude ? "" : "not ") + "present in the Docker exclude list");

                    return shouldExclude == false;
                }

                logger.warn("/etc/os-release does not exist!");
                return false;

            default:
                logger.warn("Unknown OS [" + OS.current() + "], answering false to shouldRunDockerTests()");
                return false;
        }
    }
}
