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

import org.elasticsearch.gradle.tar.SymbolicLinkPreservingUntarTask;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.artifacts.dsl.RepositoryHandler;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.Directory;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.RelativePath;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.StreamSupport;

import static org.elasticsearch.gradle.util.GradleUtils.findByName;
import static org.elasticsearch.gradle.util.GradleUtils.maybeCreate;

public class JdkDownloadPlugin implements Plugin<Project> {

    private static final String REPO_NAME_PREFIX = "jdk_repo_";
    private static final String EXTENSION_NAME = "jdks";

    @Override
    public void apply(Project project) {
        NamedDomainObjectContainer<Jdk> jdksContainer = project.container(
            Jdk.class,
            name -> new Jdk(name, project.getConfigurations().create("jdk_" + name), project.getObjects())
        );
        project.getExtensions().add(EXTENSION_NAME, jdksContainer);

        project.afterEvaluate(p -> {
            for (Jdk jdk : jdksContainer) {
                jdk.finalizeValues();

                // depend on the jdk directory "artifact" from the root project
                DependencyHandler dependencies = project.getDependencies();
                Map<String, Object> depConfig = new HashMap<>();
                depConfig.put("path", ":"); // root project
                depConfig.put(
                    "configuration",
                    configName("extracted_jdk", jdk.getVendor(), jdk.getVersion(), jdk.getPlatform(), jdk.getArchitecture())
                );
                project.getDependencies().add(jdk.getConfigurationName(), dependencies.project(depConfig));

                // ensure a root level jdk download task exists
                setupRootJdkDownload(project.getRootProject(), jdk);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<Jdk> getContainer(Project project) {
        return (NamedDomainObjectContainer<Jdk>) project.getExtensions().getByName(EXTENSION_NAME);
    }

    private static void setupRootJdkDownload(Project rootProject, Jdk jdk) {
        String extractTaskName = String.format(
            Locale.ROOT,
            "extract-%s-%s-jdk-%s-%s",
            jdk.getPlatform(),
            jdk.getArchitecture(),
            jdk.getVendor(),
            jdk.getVersion()
        );

        // Skip setup if we've already configured a JDK for this platform, vendor and version
        if (findByName(rootProject.getTasks(), extractTaskName) == null) {
            RepositoryHandler repositories = rootProject.getRepositories();

            /*
             * Define the appropriate repository for the given JDK vendor and version
             *
             * For Oracle/OpenJDK/AdoptOpenJDK we define a repository per-version.
             */
            String repoName = REPO_NAME_PREFIX + jdk.getVendor() + "_" + jdk.getVersion();
            String repoUrl;
            String artifactPattern;

            if (jdk.getVendor().equals("adoptopenjdk")) {
                repoUrl = "https://api.adoptopenjdk.net/v3/binary/version/";
                if (jdk.getMajor().equals("8")) {
                    // legacy pattern for JDK 8
                    artifactPattern = "jdk"
                        + jdk.getBaseVersion()
                        + "-"
                        + jdk.getBuild()
                        + "/[module]/[classifier]/jdk/hotspot/normal/adoptopenjdk";
                } else {
                    // current pattern since JDK 9
                    artifactPattern = "jdk-"
                        + jdk.getBaseVersion()
                        + "+"
                        + jdk.getBuild()
                        + "/[module]/[classifier]/jdk/hotspot/normal/adoptopenjdk";
                }
            } else if (jdk.getVendor().equals("openjdk")) {
                repoUrl = "https://download.oracle.com";
                if (jdk.getHash() != null) {
                    // current pattern since 12.0.1
                    artifactPattern = "java/GA/jdk"
                        + jdk.getBaseVersion()
                        + "/"
                        + jdk.getHash()
                        + "/"
                        + jdk.getBuild()
                        + "/GPL/openjdk-[revision]_[module]-[classifier]_bin.[ext]";
                } else {
                    // simpler legacy pattern from JDK 9 to JDK 12 that we are advocating to Oracle to bring back
                    artifactPattern = "java/GA/jdk"
                        + jdk.getMajor()
                        + "/"
                        + jdk.getBuild()
                        + "/GPL/openjdk-[revision]_[module]-[classifier]_bin.[ext]";
                }
            } else {
                throw new GradleException("Unknown JDK vendor [" + jdk.getVendor() + "]");
            }

            // Define the repository if we haven't already
            if (repositories.findByName(repoName) == null) {
                IvyArtifactRepository ivyRepo = repositories.ivy(repo -> {
                    repo.setName(repoName);
                    repo.setUrl(repoUrl);
                    repo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
                    repo.patternLayout(layout -> layout.artifact(artifactPattern));
                });
                repositories.exclusiveContent(exclusiveContentRepository -> {
                    exclusiveContentRepository.filter(config -> config.includeGroup(groupName(jdk)));
                    exclusiveContentRepository.forRepositories(ivyRepo);
                });
            }

            // Declare a configuration and dependency from which to download the remote JDK
            final ConfigurationContainer configurations = rootProject.getConfigurations();
            String downloadConfigName = configName(jdk.getVendor(), jdk.getVersion(), jdk.getPlatform(), jdk.getArchitecture());
            Configuration downloadConfiguration = maybeCreate(configurations, downloadConfigName);
            rootProject.getDependencies().add(downloadConfigName, dependencyNotation(jdk));

            // Create JDK extract task
            final Provider<Directory> extractPath = rootProject.getLayout()
                .getBuildDirectory()
                .dir("jdks/" + jdk.getVendor() + "-" + jdk.getBaseVersion() + "_" + jdk.getPlatform() + "_" + jdk.getArchitecture());

            TaskProvider<?> extractTask = createExtractTask(
                extractTaskName,
                rootProject,
                jdk.getPlatform(),
                downloadConfiguration,
                extractPath
            );

            // Declare a configuration for the extracted JDK archive
            String artifactConfigName = configName(
                "extracted_jdk",
                jdk.getVendor(),
                jdk.getVersion(),
                jdk.getPlatform(),
                jdk.getArchitecture()
            );
            maybeCreate(configurations, artifactConfigName);
            rootProject.getArtifacts().add(artifactConfigName, extractPath, artifact -> artifact.builtBy(extractTask));
        }
    }

    private static TaskProvider<?> createExtractTask(
        String taskName,
        Project rootProject,
        String platform,
        Configuration downloadConfiguration,
        Provider<Directory> extractPath
    ) {
        if (platform.equals("windows")) {
            final Callable<FileTree> fileGetter = () -> rootProject.zipTree(downloadConfiguration.getSingleFile());
            // TODO: look into doing this as an artifact transform, which are cacheable starting in gradle 5.3
            Action<CopySpec> removeRootDir = copy -> {
                // remove extra unnecessary directory levels
                copy.eachFile(details -> {
                    Path newPathSegments = trimArchiveExtractPath(details.getRelativePath().getPathString());
                    String[] segments = StreamSupport.stream(newPathSegments.spliterator(), false)
                        .map(Path::toString)
                        .toArray(String[]::new);
                    details.setRelativePath(new RelativePath(true, segments));
                });
                copy.setIncludeEmptyDirs(false);
            };

            return rootProject.getTasks().register(taskName, Copy.class, copyTask -> {
                copyTask.doFirst(new Action<Task>() {
                    @Override
                    public void execute(Task t) {
                        rootProject.delete(extractPath);
                    }
                });
                copyTask.into(extractPath);
                copyTask.from(fileGetter, removeRootDir);
            });
        } else {
            /*
             * Gradle TarFileTree does not resolve symlinks, so we have to manually extract and preserve the symlinks.
             * cf. https://github.com/gradle/gradle/issues/3982 and https://discuss.gradle.org/t/tar-and-untar-losing-symbolic-links/2039
             */
            return rootProject.getTasks().register(taskName, SymbolicLinkPreservingUntarTask.class, task -> {
                task.getTarFile().fileProvider(rootProject.provider(downloadConfiguration::getSingleFile));
                task.getExtractPath().set(extractPath);
                task.setTransform(JdkDownloadPlugin::trimArchiveExtractPath);
            });
        }
    }

    /*
     * We want to remove up to the and including the jdk-.* relative paths. That is a JDK archive is structured as:
     *   jdk-12.0.1/
     *   jdk-12.0.1/Contents
     *   ...
     *
     * and we want to remove the leading jdk-12.0.1. Note however that there could also be a leading ./ as in
     *   ./
     *   ./jdk-12.0.1/
     *   ./jdk-12.0.1/Contents
     *
     * so we account for this and search the path components until we find the jdk-12.0.1, and strip the leading components.
     */
    private static Path trimArchiveExtractPath(String relativePath) {
        final Path entryName = Paths.get(relativePath);
        int index = 0;
        for (; index < entryName.getNameCount(); index++) {
            if (entryName.getName(index).toString().matches("jdk-?\\d.*")) {
                break;
            }
        }
        if (index + 1 >= entryName.getNameCount()) {
            // this happens on the top-level directories in the archive, which we are removing
            return null;
        }
        // finally remove the top-level directories from the output path
        return entryName.subpath(index + 1, entryName.getNameCount());
    }

    private static String dependencyNotation(Jdk jdk) {
        String platformDep = jdk.getPlatform().equals("darwin") || jdk.getPlatform().equals("osx")
            ? (jdk.getVendor().equals("adoptopenjdk") ? "mac" : "osx")
            : jdk.getPlatform();
        String extension = jdk.getPlatform().equals("windows") ? "zip" : "tar.gz";

        return groupName(jdk) + ":" + platformDep + ":" + jdk.getBaseVersion() + ":" + jdk.getArchitecture() + "@" + extension;
    }

    private static String groupName(Jdk jdk) {
        return jdk.getVendor() + "_" + jdk.getMajor();
    }

    private static String configName(String... parts) {
        return String.join("_", parts);
    }
}
