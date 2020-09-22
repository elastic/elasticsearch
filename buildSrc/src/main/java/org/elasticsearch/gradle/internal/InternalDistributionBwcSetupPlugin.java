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

package org.elasticsearch.gradle.internal;

import org.apache.commons.io.FileUtils;
import org.apache.tools.ant.taskdefs.condition.Os;
import org.elasticsearch.gradle.BwcVersions;
import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.logging.LogLevel;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.gradle.util.JavaUtil.getJavaHome;

/**
 *  We want to be able to do BWC tests for unreleased versions without relying on and waiting for snapshots.
 *  For this we need to check out and build the unreleased versions.
 *  Since These depend on the current version, we can't name the Gradle projects statically, and don't know what the
 *  unreleased versions are when Gradle projects are set up, so we use "build-unreleased-version-*" as placeholders
 *  and configure them to build various versions here.
 */
public class InternalDistributionBwcSetupPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        Provider<String> remote = project.getProviders().systemProperty("bwc.remote").forUseAtConfigurationTime().orElse("elastic");

        BuildParams.getBwcVersions()
            .forPreviousUnreleased(
                (BwcVersions.UnreleasedVersionInfo unreleasedVersion) -> {
                    configureProject(project.project(unreleasedVersion.gradleProjectPath), unreleasedVersion, remote);
                }
            );
    }

    private void configureProject(Project bwcProject, BwcVersions.UnreleasedVersionInfo unreleasedVersion, Provider<String> remote) {

        BwcGitExtension gitExtension = bwcProject.getPlugins().apply(InternalBwcGitPlugin.class).getGitExtension();
        String bwcBranch = unreleasedVersion.branch;
        Version bwcVersion = unreleasedVersion.version;
        File checkoutDir = new File(bwcProject.getBuildDir(), "bwc/checkout-" + bwcBranch);

        // using extra properties temporally while porting build script code over to this plugin
        ExtraPropertiesExtension extraProps = bwcProject.getExtensions().getExtraProperties();
        extraProps.set("bwcVersion", unreleasedVersion.version);
        extraProps.set("bwcBranch", bwcBranch);
        extraProps.set("checkoutDir", checkoutDir);
        extraProps.set("remote", remote.get());

        gitExtension.getBwcVersion().set(unreleasedVersion.version);
        gitExtension.getBwcBranch().set(bwcBranch);
        gitExtension.setCheckoutDir(checkoutDir);

        bwcProject.getPlugins().apply("distribution");
        // Not published so no need to assemble
        bwcProject.getTasks().named("assemble").configure(t -> t.setEnabled(false));

        TaskProvider<Task> buildBwcTaskProvider = bwcProject.getTasks().register("buildBwc");
        Map<String, File> artifactFiles = new HashMap<>();
        List<String> projectDirs = new ArrayList<>();
        List<String> projects = new ArrayList<>();
        projects.addAll(Arrays.asList("deb", "rpm"));
        if (bwcVersion.onOrAfter("7.0.0")) {
            projects.addAll(Arrays.asList("windows-zip", "darwin-tar", "linux-tar"));
        } else {
            projects.add("zip");
        }

        for (String projectName : projects) {
            String baseDir = "distribution";
            String classifier = "";
            String extension = projectName;
            if (bwcVersion.onOrAfter("7.0.0") && (projectName.contains("zip") || projectName.contains("tar"))) {
                int index = projectName.indexOf('-');
                classifier = "-" + projectName.substring(0, index) + "-x86_64";
                extension = projectName.substring(index + 1);
                if (extension.equals("tar")) {
                    extension += ".gz";
                }
            } else if (bwcVersion.onOrAfter("7.0.0") && projectName.contains("deb")) {
                classifier = "-amd64";
            } else if (bwcVersion.onOrAfter("7.0.0") && projectName.contains("rpm")) {
                classifier = "-x86_64";
            }
            if (bwcVersion.onOrAfter("6.3.0")) {
                baseDir += projectName.endsWith("zip") || projectName.endsWith("tar") ? "/archives" : "/packages";
                // add oss variant first
                projectDirs.add(baseDir + "/oss-" + projectName);
                String relativeOssPath = baseDir
                    + "/oss-"
                    + projectName
                    + "/build/distributions/elasticsearch-oss-"
                    + bwcVersion
                    + "-SNAPSHOT"
                    + classifier
                    + "."
                    + extension;

                File ossProjectArtifact = new File(checkoutDir, relativeOssPath);
                artifactFiles.put("oss-" + projectName, ossProjectArtifact);
                String projectPath = baseDir + "/oss-" + projectName;
                createBuildBwcTask(
                    bwcProject,
                    checkoutDir,
                    bwcVersion,
                    "oss-" + projectName,
                    projectPath,
                    ossProjectArtifact,
                    buildBwcTaskProvider
                );
            }
            projectDirs.add(baseDir + "/" + projectName);

            String relativePath = baseDir
                + "/"
                + projectName
                + "/build/distributions/elasticsearch-"
                + bwcVersion
                + "-SNAPSHOT"
                + classifier
                + "."
                + extension;
            File projectArtifact = new File(checkoutDir, relativePath);
            artifactFiles.put(projectName, projectArtifact);

            createBuildBwcTask(
                bwcProject,
                checkoutDir,
                bwcVersion,
                projectName,
                baseDir + "/" + projectName,
                projectArtifact,
                buildBwcTaskProvider
            );
        }

        // Create build tasks for the JDBC driver used for compatibility testing
        String jdbcProjectDir = "x-pack/plugin/sql/jdbc";

        File jdbcProjectArtifact = new File(
            checkoutDir,
            jdbcProjectDir + "/build/distributions/x-pack-sql-jdbc-" + bwcVersion + "-SNAPSHOT.jar"
        );

        createBuildBwcTask(bwcProject, checkoutDir, bwcVersion, "jdbc", jdbcProjectDir, jdbcProjectArtifact, buildBwcTaskProvider);
        TaskProvider<LoggedExec> resolveAllBwcDepsTaskProvider = createRunBwcGradleTask(
            bwcProject,
            "resolveAllBwcDependencies",
            checkoutDir,
            bwcVersion,
            t -> t.args("resolveAllDependencies")
        );

        for (Map.Entry<String, File> e : artifactFiles.entrySet()) {
            String projectName = e.getKey();
            String buildBwcTask = buildBwcTaskName(projectName);
            File artifactFile = e.getValue();
            String artifactFileName = artifactFile.getName();
            String artifactName = artifactFileName.contains("oss") ? "elasticsearch-oss" : "elasticsearch";

            String suffix = artifactFileName.endsWith("tar.gz") ? "tar.gz" : artifactFileName.substring(artifactFileName.length() - 3);
            int archIndex = artifactFileName.indexOf("x86_64");

            bwcProject.getConfigurations().create(projectName);
            bwcProject.getArtifacts().add(projectName, artifactFile, artifact -> {
                artifact.setName(artifactName);
                artifact.builtBy(buildBwcTask);
                artifact.setType(suffix);

                String classifier = "";
                if (archIndex != -1) {
                    int osIndex = artifactFileName.lastIndexOf('-', archIndex - 2);
                    classifier = "-" + artifactFileName.substring(osIndex + 1, archIndex - 1) + "-x86_64";
                }
                artifact.setClassifier(classifier);
            });
        }
        // make sure no dependencies were added to assemble; we want it to be a no-op
        bwcProject.getTasks().named("assemble").configure(t -> t.setDependsOn(Collections.emptyList()));
    }

    private TaskProvider<LoggedExec> createRunBwcGradleTask(
        Project project,
        String name,
        File checkoutDir,
        Version bwcVersion,
        Action<LoggedExec> configAction
    ) {
        return project.getTasks().register(name, LoggedExec.class, loggedExec -> {
            // TODO revisit
            loggedExec.dependsOn("checkoutBwcBranch");
            loggedExec.setSpoolOutput(true);
            loggedExec.setWorkingDir(checkoutDir);
            loggedExec.doFirst(t -> {
                // Execution time so that the checkouts are available
                String javaVersionsString = readFromFile(new File(checkoutDir, ".ci/java-versions.properties"));
                loggedExec.environment(
                    "JAVA_HOME",
                    getJavaHome(
                        Integer.parseInt(
                            javaVersionsString.lines()
                                .filter(l -> l.trim().startsWith("ES_BUILD_JAVA="))
                                .map(l -> l.replace("ES_BUILD_JAVA=java", "").trim())
                                .map(l -> l.replace("ES_BUILD_JAVA=openjdk", "").trim())
                                .collect(Collectors.joining("!!"))
                        )
                    )
                );
                loggedExec.environment(
                    "RUNTIME_JAVA_HOME",
                    getJavaHome(
                        Integer.parseInt(
                            javaVersionsString.lines()
                                .filter(l -> l.trim().startsWith("ES_RUNTIME_JAVA="))
                                .map(l -> l.replace("ES_RUNTIME_JAVA=java", "").trim())
                                .map(l -> l.replace("ES_RUNTIME_JAVA=openjdk", "").trim())
                                .collect(Collectors.joining("!!"))
                        )
                    )
                );
            });

            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                loggedExec.executable("cmd");
                loggedExec.args("/C", "call", new File(checkoutDir, "gradlew").toString());
            } else {
                loggedExec.executable(new File(checkoutDir, "gradlew").toString());
            }
            if (project.getGradle().getStartParameter().isOffline()) {
                loggedExec.args("--offline");
            }
            // TODO resolve
            String buildCacheUrl = System.getProperty("org.elasticsearch.build.cache.url");
            if (buildCacheUrl != null) {
                loggedExec.args("-Dorg.elasticsearch.build.cache.url=" + buildCacheUrl);
            }

            loggedExec.args("-Dbuild.snapshot=true");
            loggedExec.args("-Dscan.tag.NESTED");
            final LogLevel logLevel = project.getGradle().getStartParameter().getLogLevel();
            List<LogLevel> nonDefaultLogLevels = Arrays.asList(LogLevel.QUIET, LogLevel.WARN, LogLevel.INFO, LogLevel.DEBUG);
            if (nonDefaultLogLevels.contains(logLevel)) {
                loggedExec.args("--" + logLevel.name().toLowerCase(Locale.ENGLISH));
            }
            final String showStacktraceName = project.getGradle().getStartParameter().getShowStacktrace().name();
            assert Arrays.asList("INTERNAL_EXCEPTIONS", "ALWAYS", "ALWAYS_FULL").contains(showStacktraceName);
            if (showStacktraceName.equals("ALWAYS")) {
                loggedExec.args("--stacktrace");
            } else if (showStacktraceName.equals("ALWAYS_FULL")) {
                loggedExec.args("--full-stacktrace");
            }
            if (project.getGradle().getStartParameter().isParallelProjectExecutionEnabled()) {
                loggedExec.args("--parallel");
            }
            loggedExec.setStandardOutput(new IndentingOutputStream(System.out, bwcVersion));
            loggedExec.setErrorOutput(new IndentingOutputStream(System.err, bwcVersion));
            configAction.execute(loggedExec);
        });
    }

    private String readFromFile(File file) {
        try {
            return FileUtils.readFileToString(file).trim();
        } catch (IOException ioException) {
            throw new GradleException("Cannot read java properties file.", ioException);
        }
    }

    private String buildBwcTaskName(String projectName) {
        return "buildBwc"
            + Arrays.stream(projectName.split("-"))
                .map(i -> i.substring(0, 1).toUpperCase(Locale.ROOT) + i.substring(1))
                .collect(Collectors.joining());
    }

    void createBuildBwcTask(
        Project project,
        File checkoutDir,
        Version bwcVersion,
        String projectName,
        String projectPath,
        File projectArtifact,
        TaskProvider<Task> bwcTaskProvider
    ) {
        String bwcTaskName = buildBwcTaskName(projectName);

        createRunBwcGradleTask(project, bwcTaskName, checkoutDir, bwcVersion, c -> {
            c.getInputs().file(new File(project.getBuildDir(), "refspec"));
            c.getOutputs().files(projectArtifact);
            c.getOutputs().cacheIf("BWC distribution caching is disabled on 'master' branch", task -> {
                String gitBranch = System.getenv("GIT_BRANCH");
                return BuildParams.isCi() && (gitBranch == null || gitBranch.endsWith("master") == false);
            });
            c.args(projectPath.replace('/', ':') + ":assemble");
            if (project.getGradle().getStartParameter().isBuildCacheEnabled()) {
                c.args("--build-cache");
            }
            c.doLast(task -> {
                if (projectArtifact.exists() == false) {
                    throw new InvalidUserDataException("Building " + bwcVersion + " didn't generate expected file " + projectArtifact);
                }
            });
        });
        bwcTaskProvider.configure(t -> t.dependsOn(bwcTaskName));
    }

    private static class IndentingOutputStream extends OutputStream {

        public final byte[] indent;
        private final OutputStream delegate;

        IndentingOutputStream(OutputStream delegate, Object version) {
            this.delegate = delegate;
            indent = (" [" + version + "] ").getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void write(int b) throws IOException {
            int[] arr = { b };
            write(arr, 0, 1);
        }

        public void write(int[] bytes, int offset, int length) throws IOException {
            for (int i = 0; i < bytes.length; i++) {
                delegate.write(bytes[i]);
                if (bytes[i] == '\n') {
                    delegate.write(indent);
                }
            }
        }
    }
}
