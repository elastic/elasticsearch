/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.transform.SymbolicLinkPreservingUntarTransform;
import org.elasticsearch.gradle.transform.UnzipTransform;
import org.gradle.api.DefaultTask;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.plugins.JvmToolchainsPlugin;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.jvm.toolchain.JavaToolchainService;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static org.elasticsearch.gradle.internal.util.ParamsUtils.applyGlobalBuildInfoIfRoot;
import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * We want to be able to do BWC tests for unreleased versions without relying on and waiting for snapshots.
 * For this we need to check out and build the unreleased versions.
 * Since these depend on the current version, we can't name the Gradle projects statically, and don't know what the
 * unreleased versions are when Gradle projects are set up, so we use "build-unreleased-version-*" as placeholders
 * and configure them to build various versions here.
 */
public class InternalDistributionBwcSetupPlugin implements Plugin<Project> {

    private static final String WRAPPER_DISTS_RELATIVE_PATH = "wrapper/dists";
    static final Attribute<Boolean> BWC_DISTRIBUTION_ATTRIBUTE = Attribute.of("bwc-distribution", Boolean.class);

    private final ObjectFactory objectFactory;
    private final ProviderFactory providerFactory;

    @Inject
    public InternalDistributionBwcSetupPlugin(ObjectFactory objectFactory, ProviderFactory providerFactory) {
        this.objectFactory = objectFactory;
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(Project project) {
        applyGlobalBuildInfoIfRoot(project);
        project.getPlugins().apply(JvmToolchainsPlugin.class);
        JavaToolchainService toolChainService = project.getExtensions().getByType(JavaToolchainService.class);
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
                isCi
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
                isCi
            );
        }

        // We need source access to unmaintained previous major for our yamlRestTest compatibility tests but we do not
        // want bwc coverage for this atm.
        Optional<BwcVersions.UnreleasedVersionInfo> unmaintainedPreviousMajor = buildParams.getBwcVersions().getUnmaintainedPreviousMajor();
        if (unmaintainedPreviousMajor.isPresent()) {
            configureBwcProject(
                project.project(unmaintainedPreviousMajor.get().gradleProjectPath()),
                buildParams,
                unmaintainedPreviousMajor.get(),
                providerFactory,
                objectFactory,
                toolChainService,
                isCi
            );
        }
        // In a scenario where we do not have unreleased previous major we still wanna resolve some resources directly from branch
        // (e.g. needed by YamlRestTestCompatibilityTestPlugin)

    }

    private static void configureBwcProject(
        Project project,
        BuildParameterExtension buildParams,
        BwcVersions.UnreleasedVersionInfo versionInfo,
        ProviderFactory providerFactory,
        ObjectFactory objectFactory,
        JavaToolchainService toolChainService,
        Boolean isCi
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
        InternalBwcGitPlugin bwcGitPlugin = project.getPlugins().apply(InternalBwcGitPlugin.class);
        BwcGitExtension gitExtension = bwcGitPlugin.getGitExtension();
        Provider<Version> bwcVersion = versionInfoProvider.map(info -> info.version());

        // Register DRA artifact types and transforms for this BWC sub-project.
        registerDraArtifactTransforms(project);

        // Set the git extension properties before creating the pre-built snapshot provider, so the provider's
        // ValueSource parameters receive proper lazy Provider<String> values.
        gitExtension.setBwcVersion(versionInfoProvider.map(info -> info.version()));
        gitExtension.setBwcBranch(versionInfoProvider.map(info -> info.branch()));
        gitExtension.getCheckoutDir().set(checkoutDir);

        Provider<String> remote = providerFactory.systemProperty("bwc.remote").orElse("elastic");
        Provider<String> draBaseUrl = providerFactory.systemProperty("tests.bwc.dra.base.url")
            .orElse("https://artifacts-snapshot.elastic.co");
        Provider<String> mavenSnapshotsBaseUrl = providerFactory.systemProperty("tests.bwc.mavensnapshot.base.url")
            .orElse("https://snapshots.elastic.co/maven");
        String bwcMode = providerFactory.systemProperty("tests.bwc.mode").getOrElse("gradle");
        validateBwcMode(bwcMode);
        Provider<String> bwcHashOverride = versionInfoProvider.map(info -> info.branch())
            .flatMap(branch -> providerFactory.systemProperty("tests.bwc.dra.hash." + branch))
            .orElse("");
        Provider<String> draBuildId = providerFactory.of(DraSnapshotBuildIdValueSource.class, spec -> {
            spec.getParameters().getVersion().set(versionInfoProvider.map(info -> info.version().toString()));
            Provider<String> branch = versionInfoProvider.map(info -> info.branch());
            spec.getParameters().getBranch().set(branch);
            spec.getParameters().getRootProjectDir().set(project.getRootProject().getLayout().getProjectDirectory().getAsFile());
            spec.getParameters().getRemote().set(remote);
            spec.getParameters().getMode().set(providerFactory.systemProperty("tests.bwc.mode").orElse("gradle"));
            // -Dtests.bwc.dra.hash.{branch} overrides the local remote-tracking ref lookup,
            // allowing the pre-built snapshot fast path on stale or absent refs.
            spec.getParameters().getHashOverride().set(bwcHashOverride);
            spec.getParameters().getBaseUrl().set(draBaseUrl);
        });
        bwcGitPlugin.configureDraBuildId(draBuildId);

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
            String wrapperDistributionPath = WRAPPER_DISTS_RELATIVE_PATH + "/" + currentWrapperDistributionDirName(project);
            task.doLast(t -> {
                File uniqueGradleUserHome = new File(gradleUserHome.getAbsolutePath() + "-" + projectName);
                seedUniqueGradleUserHome(gradleUserHome, uniqueGradleUserHome, wrapperDistributionPath);
            });
        });

        for (DistributionProject distributionProject : distributionProjects) {
            createBuildBwcTask(
                bwcSetupExtension,
                buildParams,
                project,
                bwcVersion,
                BwcArtifact.distribution(
                    distributionProject.name,
                    distributionProject.projectPath,
                    distributionProject.expectedBuildArtifact,
                    distributionProject.getAssembleTaskName(),
                    distributionProject.gradleClassifier,
                    distributionProject.extension
                ),
                buildBwcTaskProvider,
                draBuildId,
                bwcHashOverride,
                draBaseUrl,
                mavenSnapshotsBaseUrl
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
            BwcArtifact.mavenArtifact(
                "jdbc",
                jdbcProjectDir,
                jdbcProjectArtifact,
                "assemble",
                "jar",
                "org.elasticsearch.plugin",
                "x-pack-sql-jdbc"
            ),
            buildBwcTaskProvider,
            draBuildId,
            bwcHashOverride,
            draBaseUrl,
            mavenSnapshotsBaseUrl
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

            String stableMavenModule = "elasticsearch-" + stableApiProject.getName();
            String stableMavenGroup = stableApiProject.getName().startsWith("plugin-") ? "org.elasticsearch.plugin" : "org.elasticsearch";
            createBuildBwcTask(
                bwcSetupExtension,
                buildParams,
                project,
                bwcVersion,
                BwcArtifact.mavenArtifact(
                    stableApiProject.getName(),
                    "libs/" + stableApiProject.getName(),
                    stableAnalysisPluginProjectArtifact,
                    "assemble",
                    "jar",
                    stableMavenGroup,
                    stableMavenModule
                ),
                buildBwcTaskProvider,
                draBuildId,
                bwcHashOverride,
                draBaseUrl,
                mavenSnapshotsBaseUrl
            );
        }
    }

    private static void registerDraArtifactTransforms(Project project) {
        project.getDependencies().getArtifactTypes().maybeCreate("tar.gz");
        project.getDependencies().registerTransform(SymbolicLinkPreservingUntarTransform.class, spec -> {
            spec.getFrom().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, "tar.gz").attribute(BWC_DISTRIBUTION_ATTRIBUTE, true);
            spec.getTo()
                .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
                .attribute(BWC_DISTRIBUTION_ATTRIBUTE, true);
            spec.parameters(p -> {});
        });
        project.getDependencies().getArtifactTypes().maybeCreate(ArtifactTypeDefinition.ZIP_TYPE);
        project.getDependencies().registerTransform(UnzipTransform.class, spec -> {
            spec.getFrom()
                .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.ZIP_TYPE)
                .attribute(BWC_DISTRIBUTION_ATTRIBUTE, true);
            spec.getTo()
                .attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
                .attribute(BWC_DISTRIBUTION_ATTRIBUTE, true);
            spec.parameters(p -> {});
        });
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

    private static String currentWrapperDistributionDirName(Project project) {
        return "gradle-" + project.getGradle().getGradleVersion() + "-bin";
    }

    /**
     * Seeds the unique Gradle user home of a nested BWC build with the Gradle config and the wrapper distribution of the outer build.
     * <p>
     * The unique user home is updated in place and never deleted as a whole: it also holds the nested build's dependency caches and
     * the registry of its daemons. An idle nested daemon left over from a previous build keeps writing to its registry under
     * {@code daemon/}, so deleting the whole directory races with that daemon and fails intermittently.
     */
    static void seedUniqueGradleUserHome(File sourceGradleUserHome, File uniqueGradleUserHome, String wrapperDistributionPath) {
        File wrapperSeedLock = new File(uniqueGradleUserHome.getParentFile(), uniqueGradleUserHome.getName() + ".seed.lock");
        withExclusiveFileLock(wrapperSeedLock, () -> {
            FileUtils.forceMkdir(uniqueGradleUserHome);
            refreshGradleUserHomeConfig(sourceGradleUserHome, uniqueGradleUserHome);
            File sourceWrapperDistributionDir = new File(sourceGradleUserHome, wrapperDistributionPath);
            File targetWrapperDistributionDir = new File(uniqueGradleUserHome, wrapperDistributionPath);
            // A distribution that is not ready is a leftover of an interrupted download; no nested build can be fetching it concurrently
            // as this task runs before any nested build using this user home is started.
            if (isReadyWrapperDistribution(targetWrapperDistributionDir) == false
                && isReadyWrapperDistribution(sourceWrapperDistributionDir)) {
                replaceDirectory(sourceWrapperDistributionDir, targetWrapperDistributionDir);
            }
        });
    }

    private static void refreshGradleUserHomeConfig(File sourceGradleUserHome, File targetGradleUserHome) throws IOException {
        File gradleProperties = new File(sourceGradleUserHome, "gradle.properties");
        File targetGradleProperties = new File(targetGradleUserHome, gradleProperties.getName());
        if (gradleProperties.isFile()) {
            FileUtils.copyFile(gradleProperties, targetGradleProperties);
        } else {
            FileUtils.deleteQuietly(targetGradleProperties);
        }
        File initScriptsDir = new File(sourceGradleUserHome, "init.d");
        File targetInitScriptsDir = new File(targetGradleUserHome, initScriptsDir.getName());
        if (initScriptsDir.isDirectory()) {
            syncDirectory(initScriptsDir, targetInitScriptsDir);
        } else {
            FileUtils.deleteDirectory(targetInitScriptsDir);
        }
    }

    /**
     * Makes {@code target} mirror {@code source} without ever removing {@code target} itself: files are replaced through an atomic
     * rename, so a concurrently starting nested build sees either the old or the new content of a file, never a missing directory or
     * a partially written file. Files that no longer exist in {@code source} are removed.
     */
    private static void syncDirectory(File source, File target) throws IOException {
        FileUtils.forceMkdir(target);
        Set<Path> sourceFiles = new HashSet<>();
        for (File file : FileUtils.listFiles(source, null, true)) {
            Path relative = source.toPath().relativize(file.toPath());
            sourceFiles.add(relative);
            File targetFile = target.toPath().resolve(relative).toFile();
            FileUtils.forceMkdir(targetFile.getParentFile());
            File staging = new File(targetFile.getParentFile(), targetFile.getName() + ".tmp-" + UUID.randomUUID());
            try {
                FileUtils.copyFile(file, staging);
                try {
                    Files.move(staging.toPath(), targetFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                } catch (FileSystemException e) {
                    // An atomic rename over an existing file is not supported everywhere (e.g. it can be refused on Windows).
                    Files.move(staging.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                FileUtils.deleteQuietly(staging);
            }
        }
        for (File existing : FileUtils.listFiles(target, null, true)) {
            if (sourceFiles.contains(target.toPath().relativize(existing.toPath())) == false) {
                FileUtils.deleteQuietly(existing);
            }
        }
    }

    /**
     * Replaces {@code target} with a copy of {@code source}, staging the copy next to the target so that the target is either
     * absent or complete, but never partially copied.
     */
    private static void replaceDirectory(File source, File target) throws IOException {
        File staging = new File(target.getParentFile(), target.getName() + ".tmp-" + UUID.randomUUID());
        try {
            FileUtils.copyDirectory(source, staging);
            FileUtils.deleteDirectory(target);
            Files.move(staging.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE);
        } finally {
            FileUtils.deleteQuietly(staging);
        }
    }

    static boolean isReadyWrapperDistribution(File wrapperDistributionDir) {
        if (wrapperDistributionDir.isDirectory() == false) {
            return false;
        }
        String distributionDirName = wrapperDistributionDir.getName();
        String extractedGradleDirName = distributionDirName.endsWith("-bin")
            ? distributionDirName.substring(0, distributionDirName.length() - "-bin".length())
            : distributionDirName;
        File[] hashDirs = wrapperDistributionDir.listFiles(File::isDirectory);
        if (hashDirs == null || hashDirs.length == 0) {
            return false;
        }
        for (File hashDir : hashDirs) {
            if (new File(hashDir, distributionDirName + ".zip.ok").isFile()
                && new File(hashDir, extractedGradleDirName + "/bin/gradle").isFile()) {
                return true;
            }
        }
        return false;
    }

    private static void withExclusiveFileLock(File lockFile, CheckedRunnable action) {
        lockFile.getParentFile().mkdirs();
        try (
            FileChannel channel = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            FileLock ignored = channel.lock()
        ) {
            action.run();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to seed Gradle wrapper cache under " + lockFile.getParent(), e);
        }
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws IOException;
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
        BwcArtifact artifact,
        TaskProvider<Task> bwcTaskProvider,
        Provider<String> draBuildId,
        Provider<String> bwcHashOverride,
        Provider<String> draBaseUrl,
        Provider<String> mavenSnapshotsBaseUrl
    ) {
        String bwcTaskName = buildBwcTaskName(artifact.projectName());
        File expectedOutputFile = artifact.expectedOutputFile(bwcVersion.get(), isReleaseBuild());
        BwcMode bwcMode = BwcMode.from(project.getProviders().systemProperty("tests.bwc.mode").getOrElse(BwcMode.GRADLE.value()));

        // Evaluate the pre-built snapshot build ID at configuration time. For "gradle" mode the ValueSource
        // returns "" immediately with no network activity.
        String buildId = draBuildId.get();
        if (buildId.isEmpty() == false && artifact.supportsPrebuiltSnapshots()) {
            createPrebuiltBwcTask(
                project,
                artifact,
                bwcTaskName,
                draBaseUrl.get(),
                mavenSnapshotsBaseUrl.get(),
                buildId,
                bwcHashOverride.get().isEmpty() == false,
                bwcVersion,
                expectedOutputFile,
                buildParams,
                bwcTaskProvider
            );
        } else {
            createSourceBuildBwcTask(
                project,
                artifact,
                bwcTaskName,
                bwcSetupExtension,
                bwcVersion,
                expectedOutputFile,
                buildParams,
                bwcTaskProvider,
                bwcMode
            );
        }
    }

    private static boolean isReleaseBuild() {
        return System.getProperty("tests.bwc.snapshot", "true").equals("false");
    }

    private static void createSourceBuildBwcTask(
        Project project,
        BwcArtifact artifact,
        String bwcTaskName,
        BwcSetupExtension bwcSetupExtension,
        Provider<Version> bwcVersion,
        File expectedOutputFile,
        BuildParameterExtension buildParams,
        TaskProvider<Task> bwcTaskProvider,
        BwcMode bwcMode
    ) {
        // Evaluate bwcMode at configuration time so the value is captured in the doFirst
        // action rather than the Project instance (which is not CC-serializable).
        bwcSetupExtension.bwcTask(bwcTaskName, c -> {
            c.doFirst(task -> bwcMode.logSourceBuildFallback(task, artifact, bwcVersion.get().toString()));
            c.getInputs().file(new File(project.getBuildDir(), "refspec")).withPathSensitivity(PathSensitivity.RELATIVE);
            if (artifact.useNativeExpanded()) {
                c.getOutputs().dir(expectedOutputFile);
            } else {
                c.getOutputs().files(expectedOutputFile);
            }
            c.getOutputs().doNotCacheIf("BWC distribution caching is disabled for local builds", task -> buildParams.getCi() == false);
            c.getArgs().add("-p");
            c.getArgs().add(artifact.projectPath());
            c.getArgs().add(artifact.assembleTaskName());
            if (project.getGradle().getStartParameter().isBuildCacheEnabled()) {
                c.getArgs().add("--build-cache");
            }
            File rootDir = project.getLayout().getSettingsDirectory().getAsFile();
            c.doLast(
                task -> assertArtifactExists(
                    rootDir,
                    expectedOutputFile,
                    bwcVersion.get(),
                    "Building %s didn't generate expected artifact [%s]. The working branch may be out-of-date - try merging in the latest upstream changes to the branch."
                )
            );
        });
        bwcTaskProvider.configure(t -> t.dependsOn(bwcTaskName));
    }

    private static void createPrebuiltBwcTask(
        Project project,
        BwcArtifact artifact,
        String bwcTaskName,
        String draBaseUrl,
        String mavenSnapshotsBaseUrl,
        String buildId,
        boolean hasHashOverride,
        Provider<Version> bwcVersion,
        File expectedOutputFile,
        BuildParameterExtension buildParams,
        TaskProvider<Task> bwcTaskProvider
    ) {
        Configuration prebuiltConfig = createPrebuiltResolvableConfiguration(project, artifact, bwcVersion.get());
        String dependencyNotation = artifact.dependencyNotation(bwcVersion.get());
        project.getDependencies().add(prebuiltConfig.getName(), dependencyNotation);

        if (artifact.isMavenArtifact()) {
            registerMavenSnapshotsRepository(project, mavenSnapshotsBaseUrl);
            registerMavenSnapshotDownloadTask(
                project,
                artifact,
                bwcTaskName,
                buildId,
                hasHashOverride,
                bwcVersion,
                dependencyNotation,
                prebuiltConfig,
                buildParams
            );
        } else {
            registerDraIvyRepository(project, artifact, draBaseUrl, buildId, bwcVersion.get());
            registerDistributionDraDownloadTask(
                project,
                artifact,
                bwcTaskName,
                buildId,
                bwcVersion,
                expectedOutputFile,
                prebuiltConfig,
                buildParams
            );
        }
        bwcTaskProvider.configure(t -> t.dependsOn(bwcTaskName));
    }

    private static Configuration createPrebuiltResolvableConfiguration(Project project, BwcArtifact artifact, Version bwcVersion) {
        // Resolvable configuration: requests directory type for archives that support expansion,
        // or the raw artifact type for packages (deb, rpm) and older archives.
        Configuration prebuiltConfig = project.getConfigurations().create(artifact.prebuiltConfigurationName(bwcVersion));
        prebuiltConfig.setCanBeResolved(true);
        prebuiltConfig.setCanBeConsumed(false);
        prebuiltConfig.getResolutionStrategy().cacheChangingModulesFor(1, TimeUnit.HOURS);
        prebuiltConfig.getAttributes().attribute(BWC_DISTRIBUTION_ATTRIBUTE, true);
        prebuiltConfig.getAttributes()
            .attribute(
                ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE,
                artifact.useNativeExpanded() ? ArtifactTypeDefinition.DIRECTORY_TYPE : artifact.extension()
            );
        return prebuiltConfig;
    }

    private static void registerMavenSnapshotsRepository(Project project, String mavenSnapshotsBaseUrl) {
        String repoName = "maven-snapshots-elasticsearch";
        if (project.getRepositories().findByName(repoName) == null) {
            project.getRepositories().maven(repo -> {
                repo.setName(repoName);
                repo.setUrl(mavenSnapshotsBaseUrl);
                repo.metadataSources(s -> s.artifact());
                repo.content(c -> c.includeGroupAndSubgroups("org.elasticsearch"));
            });
        }
    }

    private static void registerMavenSnapshotDownloadTask(
        Project project,
        BwcArtifact artifact,
        String bwcTaskName,
        String buildId,
        boolean hasHashOverride,
        Provider<Version> bwcVersion,
        String dependencyNotation,
        Configuration prebuiltConfig,
        BuildParameterExtension buildParams
    ) {
        // Maven JAR artifacts (JDBC, stable API): use a dedicated task type so that
        // getOutputs().getFiles() contains exactly the downloaded JAR. A Copy task's
        // implicit @OutputDirectory would also appear in the output file set, causing
        // JarApiComparisonTask's single-jar assertion to fail with both the JAR name
        // and the parent directory name ("distributions") in the set.
        // A typed task class also avoids Configuration Cache issues: a plain
        // DefaultTask doLast lambda cannot capture a Configuration object.
        project.getTasks().register(bwcTaskName, DownloadMavenJarTask.class, task -> {
            task.doFirst(t -> {
                if (hasHashOverride) {
                    t.getLogger()
                        .warn(
                            "BWC [{}]: tests.bwc.dra.hash is set, but Maven snapshot artifact [{}] still resolves the mutable version-SNAPSHOT coordinate from snapshots.elastic.co/maven; the pinned build ID [{}] does not pin this JAR",
                            bwcVersion.get(),
                            dependencyNotation,
                            buildId
                        );
                }
                t.getLogger()
                    .lifecycle(
                        "BWC [{}]: downloading pre-built Maven snapshot artifact [{}] from snapshots.elastic.co (DRA snapshot {}) — skipping source build",
                        bwcVersion.get(),
                        dependencyNotation,
                        buildId
                    );
            });
            task.getDraJar().from(prebuiltConfig);
            task.getDraBuildId().set(buildId);
            task.getOutputJar().set(artifact.projectArtifact().distFile);
            task.getOutputs().doNotCacheIf("BWC distribution caching is disabled for local builds", t -> buildParams.getCi() == false);
        });
    }

    private static void registerDraIvyRepository(
        Project project,
        BwcArtifact artifact,
        String draBaseUrl,
        String buildId,
        Version bwcVersion
    ) {
        String repoName = artifact.draRepositoryName(bwcVersion);
        if (project.getRepositories().findByName(repoName) == null) {
            project.getRepositories().ivy(repo -> {
                repo.setName(repoName);
                repo.setUrl(draBaseUrl);
                repo.patternLayout(p -> p.artifact(artifact.distributionDraArtifactPattern(buildId)));
                repo.metadataSources(s -> s.artifact());
                repo.content(c -> c.includeVersionByRegex(artifact.effectiveMavenGroup(), artifact.effectiveMavenModule(), ".*-SNAPSHOT"));
            });
        }
    }

    private static void registerDistributionDraDownloadTask(
        Project project,
        BwcArtifact artifact,
        String bwcTaskName,
        String buildId,
        Provider<Version> bwcVersion,
        File expectedOutputFile,
        Configuration prebuiltConfig,
        BuildParameterExtension buildParams
    ) {
        // Distribution archives: use a Copy task which handles tar.gz/zip extraction
        // via registered artifact transforms.
        project.getTasks().register(bwcTaskName, Copy.class, t -> {
            t.doFirst(
                task -> task.getLogger()
                    .lifecycle(
                        "BWC [{}]: downloading pre-built distribution [{}] from DRA snapshot {} — skipping source build",
                        bwcVersion.get(),
                        artifact.projectName(),
                        buildId
                    )
            );
            t.from(prebuiltConfig);
            t.getInputs().property("draBuildId", buildId);
            if (artifact.useNativeExpanded()) {
                t.into(artifact.projectArtifact().expandedDistDir);
                t.getOutputs().dir(expectedOutputFile);
            } else {
                t.into(artifact.projectArtifact().distFile.getParentFile());
                t.getOutputs().files(artifact.projectArtifact().distFile);
            }
            t.getOutputs().doNotCacheIf("BWC distribution caching is disabled for local builds", task -> buildParams.getCi() == false);
            File rootDir = project.getLayout().getSettingsDirectory().getAsFile();
            t.doLast(
                task -> assertArtifactExists(
                    rootDir,
                    expectedOutputFile,
                    bwcVersion.get(),
                    "Downloading %s from DRA didn't produce expected artifact [%s]."
                )
            );
        });
    }

    private static void assertArtifactExists(File rootDir, File artifact, Version bwcVersion, String messageTemplate) {
        if (artifact.exists() == false) {
            Path relativeOutputPath = rootDir.toPath().relativize(artifact.toPath());
            throw new InvalidUserDataException(messageTemplate.formatted(bwcVersion, relativeOutputPath));
        }
    }

    /**
     * Validates the {@code tests.bwc.mode} value, throwing {@link InvalidUserDataException} for
     * unrecognised values so users get a clear error message rather than a silent no-op.
     */
    static void validateBwcMode(String mode) {
        BwcMode.from(mode);
    }

    /**
     * Returns the human-readable log message to emit when the source-build fallback is taken.
     * The message varies by mode (to explain <em>why</em> the fallback occurred) and by
     * artifact type (distribution archive vs. Maven JAR).
     *
     * <p>Extracted as a package-private static method so it can be exercised by unit tests without
     * standing up a full Gradle project.
     */
    static String buildFallbackMessage(String bwcMode, boolean isDistributionArchive, String bwcVersion, String projectName) {
        return BwcMode.from(bwcMode).buildFallbackMessage(isDistributionArchive, bwcVersion, projectName);
    }

    private record BwcArtifact(
        String projectName,
        String projectPath,
        DistributionProjectArtifact projectArtifact,
        String assembleTaskName,
        String gradleClassifier,
        String extension,
        String mavenGroup,
        String mavenModule
    ) {
        boolean isDistributionArchive() {
            return projectPath.startsWith("distribution/");
        }

        boolean isMavenArtifact() {
            return mavenModule.isEmpty() == false;
        }

        boolean useNativeExpanded() {
            return projectArtifact.expandedDistDir != null;
        }

        static BwcArtifact distribution(
            String projectName,
            String projectPath,
            DistributionProjectArtifact projectArtifact,
            String assembleTaskName,
            String gradleClassifier,
            String extension
        ) {
            return new BwcArtifact(projectName, projectPath, projectArtifact, assembleTaskName, gradleClassifier, extension, "", "");
        }

        static BwcArtifact mavenArtifact(
            String projectName,
            String projectPath,
            DistributionProjectArtifact projectArtifact,
            String assembleTaskName,
            String extension,
            String mavenGroup,
            String mavenModule
        ) {
            return new BwcArtifact(projectName, projectPath, projectArtifact, assembleTaskName, "", extension, mavenGroup, mavenModule);
        }

        boolean supportsPrebuiltSnapshots() {
            return isDistributionArchive() || isMavenArtifact();
        }

        File expectedOutputFile(Version bwcVersion, boolean releaseBuild) {
            return useNativeExpanded()
                ? new File(projectArtifact.expandedDistDir, "elasticsearch-" + bwcVersion + (releaseBuild ? "" : "-SNAPSHOT"))
                : projectArtifact.distFile;
        }

        String effectiveMavenGroup() {
            return mavenGroup.isEmpty() ? "org.elasticsearch" : mavenGroup;
        }

        String effectiveMavenModule() {
            return mavenModule.isEmpty() ? "elasticsearch" : mavenModule;
        }

        String dependencyNotation(Version bwcVersion) {
            String versionString = bwcVersion + "-SNAPSHOT";
            return gradleClassifier.isEmpty()
                ? effectiveMavenGroup() + ":" + effectiveMavenModule() + ":" + versionString + "@" + extension
                : effectiveMavenGroup() + ":" + effectiveMavenModule() + ":" + versionString + ":" + gradleClassifier + "@" + extension;
        }

        String prebuiltConfigurationName(Version bwcVersion) {
            return "draResolvable-" + bwcVersion + "-" + projectName;
        }

        String draRepositoryName(Version bwcVersion) {
            return "dra-bwc-elasticsearch-" + bwcVersion + "-" + projectName;
        }

        String distributionDraArtifactPattern(String buildId) {
            if (gradleClassifier.isEmpty()) {
                return "/elasticsearch/" + buildId + "/downloads/elasticsearch/[module]-[revision].[ext]";
            }
            return "/elasticsearch/" + buildId + "/downloads/elasticsearch/[module]-[revision]-[classifier].[ext]";
        }
    }

    private enum BwcMode {
        GRADLE("gradle"),
        PREBUILT("prebuilt"),
        AUTO("auto");

        private final String value;

        BwcMode(String value) {
            this.value = value;
        }

        String value() {
            return value;
        }

        static BwcMode from(String value) {
            return switch (value) {
                case "gradle" -> GRADLE;
                case "prebuilt", "dra" -> PREBUILT;
                case "auto" -> AUTO;
                default -> throw new InvalidUserDataException(
                    "Invalid tests.bwc.mode value [" + value + "]. Must be one of: gradle, prebuilt, auto"
                );
            };
        }

        String buildFallbackMessage(boolean isDistributionArchive, String bwcVersion, String projectName) {
            String prebuiltUnavailable = " — tests.bwc.mode=prebuilt but no pre-built snapshot artifacts were available"
                + " (snapshot endpoints unreachable or no build for this branch yet); falling back to source build";
            if (isDistributionArchive) {
                return switch (this) {
                    case PREBUILT -> "BWC ["
                        + bwcVersion
                        + "]: building distribution ["
                        + projectName
                        + "] from source"
                        + prebuiltUnavailable;
                    case AUTO -> "BWC ["
                        + bwcVersion
                        + "]: building distribution ["
                        + projectName
                        + "] from source"
                        + " — pre-built snapshot commit did not match local remote-tracking ref (or was unreachable)";
                    case GRADLE -> "BWC ["
                        + bwcVersion
                        + "]: building distribution ["
                        + projectName
                        + "] from source"
                        + " — set -Dtests.bwc.mode=auto to use pre-built snapshot artifacts when the commit matches";
                };
            }
            return switch (this) {
                case PREBUILT -> "BWC [" + bwcVersion + "]: building [" + projectName + "] from source" + prebuiltUnavailable;
                case GRADLE, AUTO -> "BWC [" + bwcVersion + "]: building [" + projectName + "] from source";
            };
        }

        void logSourceBuildFallback(Task task, BwcArtifact artifact, String bwcVersion) {
            String message = buildFallbackMessage(artifact.isDistributionArchive(), bwcVersion, artifact.projectName());
            if (this == PREBUILT) {
                task.getLogger().warn(message);
            } else {
                task.getLogger().lifecycle(message);
            }
        }
    }

    /**
     * Represents a distribution project (distribution/**)
     * we build from a bwc Version in a cloned repository
     */
    private static class DistributionProject {
        final String name;
        final File checkoutDir;
        final String projectPath;
        /** Classifier without the leading {@code -}, suitable for use in a Gradle dependency notation. */
        final String gradleClassifier;
        final String extension;

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
            this.gradleClassifier = classifier.isEmpty() ? "" : classifier.substring(1);
            this.extension = extension;
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

    /**
     * Downloads a single pre-built Maven JAR artifact into the declared output file location.
     *
     * <p>Using a dedicated task type (rather than {@link Copy}) ensures that
     * {@code getOutputs().getFiles()} contains exactly the one downloaded JAR, without the
     * implicit {@code @OutputDirectory} that a {@code Copy} task registers, which would break
     * {@link JarApiComparisonTask}'s single-jar assertion.
     *
     * <p>Declaring inputs as abstract properties (rather than capturing a
     * {@link org.gradle.api.artifacts.Configuration} in a lambda) keeps this task compatible
     * with Gradle's Configuration Cache and enables up-to-date checking via {@code draBuildId}.
     */
    public abstract static class DownloadMavenJarTask extends DefaultTask {

        @Input
        public abstract Property<String> getDraBuildId();

        @InputFiles
        @PathSensitive(PathSensitivity.NONE)
        public abstract ConfigurableFileCollection getDraJar();

        @OutputFile
        public abstract RegularFileProperty getOutputJar();

        @TaskAction
        public void download() throws IOException {
            File src = getDraJar().getSingleFile();
            File dest = getOutputJar().getAsFile().get();
            dest.getParentFile().mkdirs();
            Files.copy(src.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
