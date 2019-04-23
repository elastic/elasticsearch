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

package org.elasticsearch.gradle;

import groovy.lang.Closure;
import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.FileCopyDetails;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.RelativePath;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.Copy;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdkDownloadPlugin implements Plugin<Project> {

    private static final Pattern JDK_VERSION = Pattern.compile("(\\d+)(\\.\\d+\\.\\d+)?\\+(\\d+)");

    private List<String> ALLOWED_PLATFORMS = Collections.unmodifiableList(Arrays.asList("linux", "windows", "darwin"));

    @Override
    public void apply(Project project) {
        // tracking, just needed for dedup during configuration
        Set<String> versions = new HashSet<>();

        // create usesJdk task "method"
        project.getTasks().all(task ->
            task.getExtensions().findByType(ExtraPropertiesExtension.class).set("usesJdk", new Closure<Void>(project, task) {
                public void doCall(String version, String platform) {
                    if (version == null) {
                        throw new IllegalArgumentException("version must be specified to usesJdk");
                    }
                    if (platform == null) {
                        throw new IllegalArgumentException("platform must be specified to usesJdk");
                    }
                    if (ALLOWED_PLATFORMS.contains(platform) == false) {
                        throw new IllegalArgumentException("platform must be one of " + ALLOWED_PLATFORMS);
                    }
                    ExtraPropertiesExtension extraProperties = task.getExtensions().findByType(ExtraPropertiesExtension.class);
                    if (extraProperties.has("jdkHome")) {
                        throw new IllegalArgumentException("jdk version already set for task");
                    }

                    // ensure a root level jdk download task exists
                    if (versions.add(platform + version)) {
                        createRootJdkDownload(project.getRootProject(), platform, version);
                    }

                    // setup a configuration for just this version of the jdk
                    final ConfigurationContainer configurations = project.getConfigurations();
                    String configurationName = configName("localjdk", version, platform);
                    Configuration jdkConfig = configurations.findByName(configurationName);
                    if (jdkConfig == null) {
                        jdkConfig = configurations.create(configurationName);
                    }
                    task.dependsOn(jdkConfig);

                    // depend on the jdk directory "artifact" from the root project
                    DependencyHandler dependencies = project.getDependencies();
                    Map<String, Object> depConfig = new HashMap<>();
                    depConfig.put("path", ":"); // root project
                    depConfig.put("configuration", configName("localjdk", version, platform));
                    dependencies.add(configurationName, dependencies.project(depConfig));

                    // add jdkHome runtime property to the task
                    Object jdkHomeGetter = new Object() {
                        @Override
                        public String toString() {
                            return configurations.getByName(configurationName).getSingleFile().toString();
                        }
                    };
                    extraProperties.set("jdkHome", jdkHomeGetter);
                }
            })
        );
    }

    // precondition: only called once per version/platform combo
    private static void createRootJdkDownload(Project rootProject, String platform, String version) {
        String extractTaskName = "extract" + capitalize(platform) + "Jdk" + version;
        if (rootProject.getTasks().findByName(extractTaskName) != null) {
            // already setup this version
            return;
        }

        // decompose the bundled jdk version, broken into elements as: [feature, interim, update, build]
        // Note the "patch" version is not yet handled here, as it has not yet been used by java.
        Matcher jdkVersionMatcher = JDK_VERSION.matcher(version);
        if (jdkVersionMatcher.matches() == false) {
            throw new IllegalArgumentException("Malformed jdk version [" + version + "]");
        }
        String jdkVersion = jdkVersionMatcher.group(1) + (jdkVersionMatcher.group(2) != null ? (jdkVersionMatcher.group(2)) : "");
        String jdkMajor = jdkVersionMatcher.group(1);
        String jdkBuild = jdkVersionMatcher.group(3);

        // add fake ivy repo for jdk url
        String repoName = "jdk_repo_" + version;
        if (rootProject.getRepositories().findByName(repoName) == null) {
            rootProject.getRepositories().ivy(ivyRepo -> {
                ivyRepo.setName(repoName);
                ivyRepo.setUrl("https://download.java.net");
                ivyRepo.patternLayout(layout -> {
                    layout.artifact("java/GA/jdk" + jdkMajor + "/" + jdkBuild + "/GPL/openjdk-[revision]_[module]-x64_bin.[ext]");
                });
            });
        }

        // add the jdk as a "dependency"
        final ConfigurationContainer configurations = rootProject.getConfigurations();
        String remoteConfigName = configName("openjdk", version, platform);
        String localConfigName = configName("localjdk", version, platform);
        Configuration jdkConfig = configurations.findByName(remoteConfigName);
        if (jdkConfig == null) {
            jdkConfig = configurations.create(remoteConfigName);
            configurations.create(localConfigName);
        }
        String extension = platform.equals("windows") ? "zip" : "tar.gz";
        String jdkDep = "jdk:" + (platform.equals("darwin") ? "osx" : platform) + ":" + jdkVersion + "@" + extension;
        rootProject.getDependencies().add(configName("openjdk", version, platform), jdkDep);

        // add task for extraction
        int rootNdx = platform.equals("darwin") ? 2 : 1;
        Action<CopySpec> removeRootDir = copy -> {
            // remove extra unnecessary directory levels
            copy.eachFile((FileCopyDetails details) -> {
                String[] pathSegments = details.getRelativePath().getSegments();
                String[] newPathSegments = Arrays.copyOfRange(pathSegments, rootNdx, pathSegments.length);
                details.setRelativePath(new RelativePath(true, newPathSegments));
            });
            copy.setIncludeEmptyDirs(false);
        };
        // delay resolving jdkConfig until runtime
        Supplier<File> jdkArchiveGetter = jdkConfig::getSingleFile;
        final Callable<FileTree> fileGetter;
        if (extension.equals("zip")) {
            fileGetter = () -> rootProject.zipTree(jdkArchiveGetter.get());
        } else {
            fileGetter = () -> rootProject.tarTree(rootProject.getResources().gzip(jdkArchiveGetter.get()));
        }
        String extractDir = rootProject.getBuildDir().toPath().resolve("jdks/openjdk-" + jdkVersion + "_" + platform).toString();
        Task extractTask = rootProject.getTasks().create(extractTaskName, Copy.class, copyTask -> {
            copyTask.doFirst(t -> rootProject.delete(extractDir));
            copyTask.into(extractDir);
            copyTask.from(fileGetter, removeRootDir);
        });
        rootProject.getArtifacts().add(localConfigName,
            rootProject.getLayout().getProjectDirectory().dir(extractDir),
            artifact -> artifact.builtBy(extractTask));
    }

    private static String configName(String prefix, String version, String platform) {
        return prefix + "_" + version + "_" + platform;
    }

    private static String capitalize(String s) {
        return s.substring(0, 1).toUpperCase(Locale.ROOT) + s.substring(1);
    }
}
