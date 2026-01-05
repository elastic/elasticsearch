/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.gradle.api.Action;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.plugins.JvmToolchainsPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.jvm.toolchain.JavaToolchainService;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * We want to be able to do BWC tests for unreleased versions without relying on and waiting for snapshots.
 * For this we need to check out and build the unreleased versions.
 * Since these depend on the current version, we can't name the Gradle projects statically, and don't know what the
 * unreleased versions are when Gradle projects are set up, so we use "build-unreleased-version-*" as placeholders
 * and configure them to build various versions here.
 */
public class InternalDistributionBwcSetupPlugin implements Plugin<Project> {

    private final ObjectFactory objectFactory;
    private ProviderFactory providerFactory;
    private JavaToolchainService toolChainService;
    private FileSystemOperations fileSystemOperations;

    @Inject
    public InternalDistributionBwcSetupPlugin(
        ObjectFactory objectFactory,
        ProviderFactory providerFactory,
        FileSystemOperations fileSystemOperations
    ) {
        this.objectFactory = objectFactory;
        this.providerFactory = providerFactory;
        this.fileSystemOperations = fileSystemOperations;
    }

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        project.getPlugins().apply(JvmToolchainsPlugin.class);
        toolChainService = project.getExtensions().getByType(JavaToolchainService.class);
        var buildParams = loadBuildParams(project).get();
        Boolean isCi = buildParams.getCi();
        buildParams.getBwcVersions().forPreviousUnreleased((BwcVersions.UnreleasedVersionInfo unreleasedVersion) -> {
            configureBwcProject(
                project.project(unreleasedVersion.gradleProjectPath()),
                buildParams,
                unreleasedVersion,
                providerFactory,
                objectFactory,
                toolChainService,
                isCi,
                fileSystemOperations
            );
        });

        // Also set up the "main" project which is just used for arbitrary overrides. See InternalDistributionDownloadPlugin.
        if (System.getProperty("tests.bwc.main.version") != null) {
            configureBwcProject(
                project.project(":distribution:bwc:main"),
                buildParams,
                new BwcVersions.UnreleasedVersionInfo(
                    Version.fromString(System.getProperty("tests.bwc.main.version")),
                    "main",
                    ":distribution:bwc:main"
                ),
                providerFactory,
                objectFactory,
                toolChainService,
                isCi,
                fileSystemOperations
            );
        }
    }

    private static void configureBwcProject(
        Project project,
        BuildParameterExtension buildParams,
        BwcVersions.UnreleasedVersionInfo versionInfo,
        ProviderFactory providerFactory,
        ObjectFactory objectFactory,
        JavaToolchainService toolChainService,
        Boolean isCi,
        FileSystemOperations fileSystemOperations
    ) {
        ProjectLayout layout = project.getLayout();
        Provider<BwcVersions.UnreleasedVersionInfo> versionInfoProvider = providerFactory.provider(() -> versionInfo);
        Provider<File> checkoutDir = versionInfoProvider.map(
            info -> new File(layout.getBuildDirectory().get().getAsFile(), "bwc/checkout-" + info.branch())
        );
        BwcSetupExtension bwcSetupExtension = project.getExtensions()
            .create(
                "bwcSetup",
                BwcSetupExtension.class,
                project,
                objectFactory,
                providerFactory,
                toolChainService,
                versionInfoProvider,
                checkoutDir,
                isCi
            );
        BwcGitExtension gitExtension = project.getPlugins().apply(InternalBwcGitPlugin.class).getGitExtension();
        Provider<Version> bwcVersion = versionInfoProvider.map(info -> info.version());
        gitExtension.setBwcVersion(versionInfoProvider.map(info -> info.version()));
        gitExtension.setBwcBranch(versionInfoProvider.map(info -> info.branch()));
        gitExtension.getCheckoutDir().set(checkoutDir);

        // we want basic lifecycle tasks like `clean` here.
        project.getPlugins().apply(LifecycleBasePlugin.class);

        TaskProvider<Task> buildBwcTaskProvider = project.getTasks().register("buildBwc");
        List<DistributionProject> distributionProjects = resolveArchiveProjects(checkoutDir.get(), bwcVersion.get());

        // Setup gradle user home directory
        // We don't use a normal `Copy` task here as snapshotting the entire gradle user home is very expensive. This task is cheap, so
        // up-to-date checking doesn't buy us much
        project.getTasks().register("setupGradleUserHome", task -> {
            File gradleUserHome = project.getGradle().getGradleUserHomeDir();
            String projectName = project.getName();
            task.doLast(t -> {
                fileSystemOperations.copy(copy -> {
                    String absoluteGradleUserHomePath = gradleUserHome.getAbsolutePath();
                    copy.into(absoluteGradleUserHomePath + "-" + projectName);
                    copy.from(absoluteGradleUserHomePath, copySpec -> {
                        copySpec.include("gradle.properties");
                        copySpec.include("init.d/*");
                    });
                });
            });
        });

        for (DistributionProject distributionProject : distributionProjects) {
            createBuildBwcTask(
                bwcSetupExtension,
                buildParams,
                project,
                bwcVersion,
                distributionProject.name,
                distributionProject.projectPath,
                distributionProject.expectedBuildArtifact,
                buildBwcTaskProvider,
                distributionProject.getAssembleTaskName()
            );

            registerBwcDistributionArtifacts(project, distributionProject);
        }

        // Create build tasks for the JDBC driver used for compatibility testing
        String jdbcProjectDir = "x-pack/plugin/sql/jdbc";

        DistributionProjectArtifact jdbcProjectArtifact = new DistributionProjectArtifact(
            new File(checkoutDir.get(), jdbcProjectDir + "/build/distributions/x-pack-sql-jdbc-" + bwcVersion.get() + "-SNAPSHOT.jar"),
            null
        );

        createBuildBwcTask(
            bwcSetupExtension,
            buildParams,
            project,
            bwcVersion,
            "jdbc",
            jdbcProjectDir,
            jdbcProjectArtifact,
            buildBwcTaskProvider,
            "assemble"
        );

        // for versions before 8.7.0, we do not need to set up stable API bwc
        if (bwcVersion.get().before(Version.fromString("8.7.0"))) {
            return;
        }

        for (Project stableApiProject : resolveStableProjects(project)) {

            String relativeDir = project.getRootProject().relativePath(stableApiProject.getProjectDir());

            DistributionProjectArtifact stableAnalysisPluginProjectArtifact = new DistributionProjectArtifact(
                new File(
                    checkoutDir.get(),
                    relativeDir
                        + "/build/distributions/elasticsearch-"
                        + stableApiProject.getName()
                        + "-"
                        + bwcVersion.get()
                        + "-SNAPSHOT.jar"
                ),
                null
            );

            createBuildBwcTask(
                bwcSetupExtension,
                buildParams,
                project,
                bwcVersion,
                stableApiProject.getName(),
                "libs/" + stableApiProject.getName(),
                stableAnalysisPluginProjectArtifact,
                buildBwcTaskProvider,
                "assemble"
            );
        }
    }

    private static void registerBwcDistributionArtifacts(Project bwcProject, DistributionProject distributionProject) {
        String projectName = distributionProject.name;
        String buildBwcTask = buildBwcTaskName(projectName);

        registerDistributionArchiveArtifact(bwcProject, distributionProject, buildBwcTask);
        File expectedExpandedDistDirectory = distributionProject.expectedBuildArtifact.expandedDistDir;
        if (expectedExpandedDistDirectory != null) {
            String expandedDistConfiguration = "expanded-" + projectName;
            bwcProject.getConfigurations().create(expandedDistConfiguration);
            bwcProject.getArtifacts().add(expandedDistConfiguration, expectedExpandedDistDirectory, artifact -> {
                artifact.setName("elasticsearch");
                artifact.builtBy(buildBwcTask);
                artifact.setType("directory");
            });
        }
    }

    private static void registerDistributionArchiveArtifact(
        Project bwcProject,
        DistributionProject distributionProject,
        String buildBwcTask
    ) {
        File distFile = distributionProject.expectedBuildArtifact.distFile;
        String artifactFileName = distFile.getName();
        String artifactName = artifactFileName.contains("oss") ? "elasticsearch-oss" : "elasticsearch";

        String suffix = artifactFileName.endsWith("tar.gz") ? "tar.gz" : artifactFileName.substring(artifactFileName.length() - 3);
        int x86ArchIndex = artifactFileName.indexOf("x86_64");
        int aarch64ArchIndex = artifactFileName.indexOf("aarch64");

        bwcProject.getConfigurations().create(distributionProject.name);
        bwcProject.getArtifacts().add(distributionProject.name, distFile, artifact -> {
            artifact.setName(artifactName);
            artifact.builtBy(buildBwcTask);
            artifact.setType(suffix);

            String classifier = "";
            if (x86ArchIndex != -1) {
                int osIndex = artifactFileName.lastIndexOf('-', x86ArchIndex - 2);
                classifier = "-" + artifactFileName.substring(osIndex + 1, x86ArchIndex - 1) + "-x86_64";
            } else if (aarch64ArchIndex != -1) {
                int osIndex = artifactFileName.lastIndexOf('-', aarch64ArchIndex - 2);
                classifier = "-" + artifactFileName.substring(osIndex + 1, aarch64ArchIndex - 1) + "-aarch64";
            }
            artifact.setClassifier(classifier);
        });
    }

    private static List<DistributionProject> resolveArchiveProjects(File checkoutDir, Version bwcVersion) {
        List<String> projects = new ArrayList<>();
        if (bwcVersion.onOrAfter("7.13.0")) {
            projects.addAll(asList("deb", "rpm"));
            projects.addAll(asList("windows-zip", "darwin-tar", "linux-tar"));
            projects.addAll(asList("darwin-aarch64-tar", "linux-aarch64-tar"));
        } else {
            projects.addAll(asList("deb", "rpm", "oss-deb", "oss-rpm"));
            if (bwcVersion.onOrAfter("7.0.0")) { // starting with 7.0 we bundle a jdk which means we have platform-specific archives
                projects.addAll(asList("oss-windows-zip", "windows-zip", "oss-darwin-tar", "darwin-tar", "oss-linux-tar", "linux-tar"));

                // We support aarch64 for linux and mac starting from 7.12
                if (bwcVersion.onOrAfter("7.12.0")) {
                    projects.addAll(asList("oss-darwin-aarch64-tar", "oss-linux-aarch64-tar", "darwin-aarch64-tar", "linux-aarch64-tar"));
                }
            } else { // prior to 7.0 we published only a single zip and tar archives for oss and default distributions
                projects.addAll(asList("oss-zip", "zip", "tar", "oss-tar"));
            }
        }

        return projects.stream().map(name -> {
            String baseDir = "distribution" + (name.endsWith("zip") || name.endsWith("tar") ? "/archives" : "/packages");
            String classifier = "";
            String extension = name;
            if (bwcVersion.onOrAfter("7.0.0")) {
                if (name.contains("zip") || name.contains("tar")) {
                    int index = name.lastIndexOf('-');
                    String baseName = name.startsWith("oss-") ? name.substring(4, index) : name.substring(0, index);
                    classifier = "-" + baseName + (name.contains("aarch64") ? "-aarch64" : "-x86_64");
                    extension = name.substring(index + 1);
                    if (extension.equals("tar")) {
                        extension += ".gz";
                    }
                } else if (name.contains("deb")) {
                    classifier = "-amd64";
                } else if (name.contains("rpm")) {
                    classifier = "-x86_64";
                }
            }
            return new DistributionProject(name, baseDir, bwcVersion, classifier, extension, checkoutDir);
        }).collect(Collectors.toList());
    }

    private static List<Project> resolveStableProjects(Project project) {
        Set<String> stableProjectNames = Set.of("logging", "plugin-api", "plugin-analysis-api");
        return project.findProject(":libs")
            .getSubprojects()
            .stream()
            .filter(subproject -> stableProjectNames.contains(subproject.getName()))
            .toList();
    }

    public static String buildBwcTaskName(String projectName) {
        return "buildBwc"
            + stream(projectName.split("-")).map(i -> i.substring(0, 1).toUpperCase(Locale.ROOT) + i.substring(1))
                .collect(Collectors.joining());
    }

    static void createBuildBwcTask(
        BwcSetupExtension bwcSetupExtension,
        BuildParameterExtension buildParams,
        Project project,
        Provider<Version> bwcVersion,
        String projectName,
        String projectPath,
        DistributionProjectArtifact projectArtifact,
        TaskProvider<Task> bwcTaskProvider,
        String assembleTaskName
    ) {
        String bwcTaskName = buildBwcTaskName(projectName);
        bwcSetupExtension.bwcTask(bwcTaskName, c -> {
            boolean useNativeExpanded = projectArtifact.expandedDistDir != null;
            boolean isReleaseBuild = System.getProperty("tests.bwc.snapshot", "true").equals("false");
            File expectedOutputFile = useNativeExpanded
                ? new File(projectArtifact.expandedDistDir, "elasticsearch-" + bwcVersion.get() + (isReleaseBuild ? "" : "-SNAPSHOT"))
                : projectArtifact.distFile;
            c.getInputs().file(new File(project.getBuildDir(), "refspec")).withPathSensitivity(PathSensitivity.RELATIVE);
            if (useNativeExpanded) {
                c.getOutputs().dir(expectedOutputFile);
            } else {
                c.getOutputs().files(expectedOutputFile);
            }
            c.getOutputs().doNotCacheIf("BWC distribution caching is disabled for local builds", task -> buildParams.getCi() == false);
            c.getArgs().add("-p");
            c.getArgs().add(projectPath);
            c.getArgs().add(assembleTaskName);
            if (project.getGradle().getStartParameter().isBuildCacheEnabled()) {
                c.getArgs().add("--build-cache");
            }
            File rootDir = project.getRootDir();
            c.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    if (expectedOutputFile.exists() == false) {
                        Path relativeOutputPath = rootDir.toPath().relativize(expectedOutputFile.toPath());
                        final String message = "Building %s didn't generate expected artifact [%s]. The working branch may be "
                            + "out-of-date - try merging in the latest upstream changes to the branch.";
                        throw new InvalidUserDataException(message.formatted(bwcVersion.get(), relativeOutputPath));
                    }
                }
            });
        });
        bwcTaskProvider.configure(t -> t.dependsOn(bwcTaskName));
    }

    /**
     * Represents a distribution project (distribution/**)
     * we build from a bwc Version in a cloned repository
     */
    private static class DistributionProject {
        final String name;
        final File checkoutDir;
        final String projectPath;

        /**
         * can be removed once we don't build 7.10 anymore
         * from source for bwc tests.
         */
        @Deprecated
        final boolean expandedDistDirSupport;
        final DistributionProjectArtifact expectedBuildArtifact;

        private final boolean extractedAssembleSupported;

        DistributionProject(String name, String baseDir, Version version, String classifier, String extension, File checkoutDir) {
            this.name = name;
            this.checkoutDir = checkoutDir;
            this.projectPath = baseDir + "/" + name;
            this.expandedDistDirSupport = version.onOrAfter("7.10.0") && (name.endsWith("zip") || name.endsWith("tar"));
            this.extractedAssembleSupported = version.onOrAfter("7.11.0") && (name.endsWith("zip") || name.endsWith("tar"));
            this.expectedBuildArtifact = new DistributionProjectArtifact(
                new File(
                    checkoutDir,
                    baseDir
                        + "/"
                        + name
                        + "/build/distributions/elasticsearch-"
                        + (name.startsWith("oss") ? "oss-" : "")
                        + version
                        + "-SNAPSHOT"
                        + classifier
                        + "."
                        + extension
                ),
                expandedDistDirSupport ? new File(checkoutDir, baseDir + "/" + name + "/build/install") : null
            );
        }

        /**
         * Newer elasticsearch branches allow building extracted bwc elasticsearch versions
         * from source without the overhead of creating an archive by using assembleExtracted instead of assemble.
         */
        public String getAssembleTaskName() {
            return extractedAssembleSupported ? "extractedAssemble" : "assemble";
        }
    }

    private static class DistributionProjectArtifact {
        final File distFile;
        final File expandedDistDir;

        DistributionProjectArtifact(File distFile, File expandedDistDir) {
            this.distFile = distFile;
            this.expandedDistDir = expandedDistDir;
        }
    }
}
