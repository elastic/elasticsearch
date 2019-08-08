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

import org.elasticsearch.gradle.BuildPlugin;
import org.elasticsearch.gradle.BwcVersions;
import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.ElasticsearchDistribution.Flavor;
import org.elasticsearch.gradle.ElasticsearchDistribution.Platform;
import org.elasticsearch.gradle.ElasticsearchDistribution.Type;
import org.elasticsearch.gradle.Jdk;
import org.elasticsearch.gradle.JdkDownloadPlugin;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.tool.Boilerplate;
import org.elasticsearch.gradle.vagrant.BatsProgressLogger;
import org.elasticsearch.gradle.vagrant.VagrantBasePlugin;
import org.elasticsearch.gradle.vagrant.VagrantExtension;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.Directory;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskInputs;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.elasticsearch.gradle.vagrant.VagrantMachine.convertLinuxPath;
import static org.elasticsearch.gradle.vagrant.VagrantMachine.convertWindowsPath;

public class DistroTestPlugin implements Plugin<Project> {

    private static final String SYSTEM_JDK_VERSION = "11.0.2+9";
    private static final String GRADLE_JDK_VERSION = "12.0.1+12@69cfe15208a647278a19ef0990eea691";

    // all distributions used by distro tests. this is temporary until tests are per distribution
    private static final String PACKAGING_DISTRIBUTION = "packaging";
    private static final String COPY_PACKAGING_TASK = "copyPackagingArchives";
    private static final String IN_VM_SYSPROP = "tests.inVM";

    private static Version upgradeVersion;
    private Provider<Directory> archivesDir;
    private TaskProvider<Copy> copyPackagingArchives;
    private Jdk systemJdk;
    private Jdk gradleJdk;

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(JdkDownloadPlugin.class);
        project.getPluginManager().apply(DistributionDownloadPlugin.class);
        project.getPluginManager().apply(VagrantBasePlugin.class);
        project.getPluginManager().apply(JavaPlugin.class);
        
        configureVM(project);

        if (upgradeVersion == null) {
            // just read this once, since it is the same for all projects. this is safe because gradle configuration is single threaded
            upgradeVersion = getUpgradeVersion(project);
        }

        // setup task to run inside VM
        configureDistributions(project);
        configureCopyPackagingTask(project);
        configureDistroTest(project);
        configureBatsTest(project, "oss");
        configureBatsTest(project, "default");
    }

    private static Jdk createJdk(NamedDomainObjectContainer<Jdk> jdksContainer, String name, String version, String platform) {
        Jdk jdk = jdksContainer.create(name);
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

        ExtraPropertiesExtension rootExtraProperties = project.getRootProject().getExtensions().getByType(ExtraPropertiesExtension.class);
        String firstPartOfSeed = rootExtraProperties.get("testSeed").toString().split(":")[0];
        final long seed = Long.parseUnsignedLong(firstPartOfSeed, 16);
        BwcVersions bwcVersions = (BwcVersions) extraProperties.get("bwcVersions");
        final List<Version> indexCompatVersions = bwcVersions.getIndexCompatible();
        return indexCompatVersions.get(new Random(seed).nextInt(indexCompatVersions.size()));
    }

    private void configureVM(Project project) {
        String box = project.getName();

        // setup jdks used by the distro tests, and by gradle executing
        
        NamedDomainObjectContainer<Jdk> jdksContainer = JdkDownloadPlugin.getContainer(project);
        String platform = box.contains("windows") ? "windows" : "linux";
        this.systemJdk = createJdk(jdksContainer, "system", SYSTEM_JDK_VERSION, platform);
        this.gradleJdk = createJdk(jdksContainer, "gradle", GRADLE_JDK_VERSION, platform);

        // setup VM used by these tests
        VagrantExtension vagrant = project.getExtensions().getByType(VagrantExtension.class);
        vagrant.setBox(box);
        vagrant.vmEnv("SYSTEM_JAVA_HOME", convertPath(project, vagrant, systemJdk, "", ""));
        vagrant.vmEnv("PATH", convertPath(project, vagrant, gradleJdk, "/bin:$PATH", "\\bin;$Env:PATH"));
        vagrant.setIsWindowsVM(box.contains("windows"));
    }

    private static Object convertPath(Project project, VagrantExtension vagrant, Jdk jdk,
                                      String additionaLinux, String additionalWindows) {
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

    private void configureCopyPackagingTask(Project project) {
        this.archivesDir = project.getParent().getLayout().getBuildDirectory().dir("packaging/archives");
        // temporary, until we have tasks per distribution
        this.copyPackagingArchives = Boilerplate.maybeRegister(project.getParent().getTasks(), COPY_PACKAGING_TASK, Copy.class,
            t -> {
                t.into(archivesDir);
                t.from(project.getConfigurations().getByName(PACKAGING_DISTRIBUTION));

                Path archivesPath = archivesDir.get().getAsFile().toPath();

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
                inputs.property("version", VersionProperties.getElasticsearch());
                inputs.property("upgrade_from_version", upgradeFromVersion);
                // TODO: this is serializable, need to think how to represent this as an input
                //inputs.property("bwc_versions", bwcVersions);
                t.doLast(action -> {
                    try {
                        Files.writeString(archivesPath.resolve("version"), VersionProperties.getElasticsearch());
                        Files.writeString(archivesPath.resolve("upgrade_from_version"), upgradeFromVersion);
                        // this is always true, but bats tests rely on it. It is just temporary until bats is removed.
                        Files.writeString(archivesPath.resolve("upgrade_is_oss"), "");
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
        });
    }

    private void configureDistroTest(Project project) {
        BuildPlugin.configureCompile(project);
        BuildPlugin.configureRepositories(project);
        BuildPlugin.configureTestTasks(project);
        BuildPlugin.configureInputNormalization(project);

        TaskProvider<Test> destructiveTest = project.getTasks().register("destructiveDistroTest", Test.class,
            t -> {
                t.setMaxParallelForks(1);
                t.setWorkingDir(archivesDir.get());
                if (System.getProperty(IN_VM_SYSPROP) == null) {
                    t.dependsOn(copyPackagingArchives, systemJdk, gradleJdk);
                }
            });

        // setup outer task to run
        project.getTasks().register("distroTest", GradleDistroTestTask.class,
            t -> {
                t.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
                t.setDescription("Runs distribution tests within vagrant");
                t.setTaskName(project.getPath() + ":" + destructiveTest.getName());
                t.extraArg("-D'" + IN_VM_SYSPROP + "'");
                t.dependsOn(copyPackagingArchives, systemJdk, gradleJdk);
            });
    }

    private void configureBatsTest(Project project, String type) {

        // destructive task to run inside
        TaskProvider<BatsTestTask> destructiveTest = project.getTasks().register("destructiveBatsTest." + type, BatsTestTask.class,
            t -> {
                // this is hacky for shared source, but bats are a temporary thing we are removing, so it is not worth
                // the overhead of a real project dependency
                Directory batsDir = project.getParent().getLayout().getProjectDirectory().dir("bats");
                t.setTestsDir(batsDir.dir(type));
                t.setUtilsDir(batsDir.dir("utils"));
                t.setArchivesDir(archivesDir.get());
                t.setPackageName("elasticsearch" + (type.equals("oss") ? "-oss" : ""));
                if (System.getProperty(IN_VM_SYSPROP) == null) {
                    t.dependsOn(copyPackagingArchives, systemJdk, gradleJdk);
                }
            });

        VagrantExtension vagrant = project.getExtensions().getByType(VagrantExtension.class);
        // setup outer task to run
        project.getTasks().register("batsTest." + type, GradleDistroTestTask.class,
            t -> {
                t.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
                t.setDescription("Runs bats tests within vagrant");
                t.setTaskName(project.getPath() + ":" + destructiveTest.getName());
                t.setProgressHandler(new BatsProgressLogger(project.getLogger()));
                t.extraArg("-D'" + IN_VM_SYSPROP + "'");
                t.dependsOn(copyPackagingArchives, systemJdk, gradleJdk);
                t.onlyIf(spec -> vagrant.isWindowsVM() == false); // bats doesn't run on windows
            });
    }
    
    private void configureDistributions(Project project) {
        NamedDomainObjectContainer<ElasticsearchDistribution> distributions = DistributionDownloadPlugin.getContainer(project);

        for (Type type : Arrays.asList(Type.DEB, Type.RPM)) {
            for (Flavor flavor : Flavor.values()) {
                for (boolean bundledJdk : Arrays.asList(true, false)) {
                    addDistro(distributions, type, null, flavor, bundledJdk, VersionProperties.getElasticsearch());
                }
            }
            // upgrade version is always bundled jdk
            // NOTE: this is mimicking the old VagrantTestPlugin upgrade behavior. It will eventually be replaced
            // witha dedicated upgrade test from every bwc version like other bwc tests
            addDistro(distributions, type, null, Flavor.DEFAULT, true, upgradeVersion.toString());
            if (upgradeVersion.onOrAfter("6.3.0")) {
                addDistro(distributions, type, null, Flavor.OSS, true, upgradeVersion.toString());
            }
        }
        for (Platform platform : Arrays.asList(Platform.LINUX, Platform.WINDOWS)) {
            for (Flavor flavor : Flavor.values()) {
                for (boolean bundledJdk : Arrays.asList(true, false)) {
                    addDistro(distributions, Type.ARCHIVE, platform, flavor, bundledJdk, VersionProperties.getElasticsearch());
                }
            }
        }

        // temporary until distro tests have one test per distro
        Configuration packagingConfig = project.getConfigurations().create(PACKAGING_DISTRIBUTION);
        List<Configuration> distroConfigs = distributions.stream().map(ElasticsearchDistribution::getConfiguration)
            .collect(Collectors.toList());
        packagingConfig.setExtendsFrom(distroConfigs);
    }

    private static void addDistro(NamedDomainObjectContainer<ElasticsearchDistribution> distributions,
                                  Type type, Platform platform, Flavor flavor, boolean bundledJdk, String version) {

        String name = flavor + "-" + (type == Type.ARCHIVE ? platform + "-" : "") + type + (bundledJdk ? "" : "-no-jdk") + "-" + version;
        if (distributions.findByName(name) != null) {
            return;
        }
        distributions.create(name, d -> {
            d.setFlavor(flavor);
            d.setType(type);
            if (type == Type.ARCHIVE) {
                d.setPlatform(platform);
            }
            d.setBundledJdk(bundledJdk);
            d.setVersion(version);
        });
    }
}
