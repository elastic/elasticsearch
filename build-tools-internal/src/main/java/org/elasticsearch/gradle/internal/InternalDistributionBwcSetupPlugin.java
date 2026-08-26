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
import org.elasticsearch.gradle.transform.SymbolicLinkPreservingUntarTransform;
import org.elasticsearch.gradle.transform.UnzipTransform;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileSystemOperations;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
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

    static final Attribute<Boolean> BWC_DISTRIBUTION_ATTRIBUTE = Attribute.of("bwc-distribution", Boolean.class);

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
                isCi,
                fileSystemOperations
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
        InternalBwcGitPlugin bwcGitPlugin = project.getPlugins().apply(InternalBwcGitPlugin.class);
        BwcGitExtension gitExtension = bwcGitPlugin.getGitExtension();
        Provider<Version> bwcVersion = versionInfoProvider.map(info -> info.version());

        // Register DRA artifact types and transforms for this BWC sub-project.
        registerDraArtifactTransforms(project);

        // Set the git extension properties before creating the DRA provider, so the provider's
        // ValueSource parameters receive proper lazy Provider<String> values.
        gitExtension.setBwcVersion(versionInfoProvider.map(info -> info.version()));
        gitExtension.setBwcBranch(versionInfoProvider.map(info -> info.branch()));
        gitExtension.getCheckoutDir().set(checkoutDir);

        Provider<String> remote = providerFactory.systemProperty("bwc.remote").orElse("elastic");
        Provider<String> draBaseUrl = providerFactory.systemProperty("tests.bwc.dra.base.url")
            .orElse("https://artifacts-snapshot.elastic.co");
        String bwcMode = providerFactory.systemProperty("tests.bwc.mode").getOrElse("gradle");
        validateBwcMode(bwcMode);
        Provider<String> draBuildId = providerFactory.of(DraSnapshotBuildIdValueSource.class, spec -> {
            spec.getParameters().getVersion().set(versionInfoProvider.map(info -> info.version().toString()));
            Provider<String> branch = versionInfoProvider.map(info -> info.branch());
            spec.getParameters().getBranch().set(branch);
            spec.getParameters().getRootProjectDir().set(project.getRootProject().getLayout().getProjectDirectory().getAsFile());
            spec.getParameters().getRemote().set(remote);
            spec.getParameters().getMode().set(providerFactory.systemProperty("tests.bwc.mode").orElse("gradle"));
            // -Dtests.bwc.dra.hash.{branch} overrides the local remote-tracking ref lookup,
            // allowing the DRA fast path on stale or absent refs.
            spec.getParameters()
                .getHashOverride()
                .set(branch.flatMap(b -> providerFactory.systemProperty("tests.bwc.dra.hash." + b)).orElse(""));
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
                distributionProject.getAssembleTaskName(),
                draBuildId,
                distributionProject.gradleClassifier,
                distributionProject.extension,
                draBaseUrl,
                "",
                ""
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
            "assemble",
            draBuildId,
            "",
            "jar",
            draBaseUrl,
            "org.elasticsearch.plugin",
            "x-pack-sql-jdbc"
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
                stableApiProject.getName(),
                "libs/" + stableApiProject.getName(),
                stableAnalysisPluginProjectArtifact,
                buildBwcTaskProvider,
                "assemble",
                draBuildId,
                "",
                "jar",
                draBaseUrl,
                stableMavenGroup,
                stableMavenModule
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
        String assembleTaskName,
        Provider<String> draBuildId,
        String gradleClassifier,
        String extension,
        Provider<String> draBaseUrl,
        String mavenGroup,
        String mavenModule
    ) {
        String bwcTaskName = buildBwcTaskName(projectName);
        boolean useNativeExpanded = projectArtifact.expandedDistDir != null;
        boolean isReleaseBuild = System.getProperty("tests.bwc.snapshot", "true").equals("false");
        File expectedOutputFile = useNativeExpanded
            ? new File(projectArtifact.expandedDistDir, "elasticsearch-" + bwcVersion.get() + (isReleaseBuild ? "" : "-SNAPSHOT"))
            : projectArtifact.distFile;

        boolean isDistributionArchive = projectPath.startsWith("distribution/");
        // Evaluate mode and DRA build ID at configuration time. For "gradle" mode the ValueSource
        // returns "" immediately with no network activity.
        String bwcMode = project.getProviders().systemProperty("tests.bwc.mode").getOrElse("gradle");
        String buildId = draBuildId.get();
        boolean useDra = buildId.isEmpty() == false && (isDistributionArchive || mavenModule.isEmpty() == false);

        if (useDra) {
            createDraBwcTask(
                project,
                bwcTaskName,
                buildId,
                draBaseUrl.get(),
                bwcVersion,
                projectName,
                projectArtifact,
                gradleClassifier,
                extension,
                useNativeExpanded,
                expectedOutputFile,
                buildParams,
                bwcTaskProvider,
                mavenGroup,
                mavenModule
            );
        } else {
            // Evaluate bwcMode at configuration time so the value is captured in the doFirst
            // action rather than the Project instance (which is not CC-serializable).
            bwcSetupExtension.bwcTask(bwcTaskName, c -> {
                c.doFirst(task -> {
                    String msg = buildFallbackMessage(bwcMode, isDistributionArchive, bwcVersion.get().toString(), projectName);
                    if (bwcMode.equals("dra")) {
                        task.getLogger().warn(msg);
                    } else {
                        task.getLogger().lifecycle(msg);
                    }
                });
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
                File rootDir = project.getLayout().getSettingsDirectory().getAsFile();
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
    }

    private static void createDraBwcTask(
        Project project,
        String bwcTaskName,
        String buildId,
        String draBaseUrl,
        Provider<Version> bwcVersion,
        String projectName,
        DistributionProjectArtifact projectArtifact,
        String gradleClassifier,
        String extension,
        boolean useNativeExpanded,
        File expectedOutputFile,
        BuildParameterExtension buildParams,
        TaskProvider<Task> bwcTaskProvider,
        String mavenGroup,
        String mavenModule
    ) {
        String effectiveMavenGroup = mavenGroup.isEmpty() ? "org.elasticsearch" : mavenGroup;
        String effectiveMavenModule = mavenModule.isEmpty() ? "elasticsearch" : mavenModule;

        // Configure the DRA Ivy repository for this specific BWC version and build.
        String repoName = "dra-bwc-elasticsearch-" + bwcVersion.get() + "-" + projectName;
        if (project.getRepositories().findByName(repoName) == null) {
            project.getRepositories().ivy(repo -> {
                repo.setName(repoName);
                repo.setUrl(draBaseUrl);
                repo.patternLayout(p -> {
                    if (mavenGroup.isEmpty() == false) {
                        // Maven-coordinated artifacts (e.g. JDBC jar) live under the /maven/ tree
                        // using the standard Maven group-path layout rather than the flat
                        // /downloads/elasticsearch/ path used for distribution archives.
                        String groupPath = effectiveMavenGroup.replace(".", "/");
                        p.artifact("/elasticsearch/" + buildId + "/maven/" + groupPath + "/[module]/[revision]/[module]-[revision].[ext]");
                    } else if (gradleClassifier.isEmpty()) {
                        p.artifact("/elasticsearch/" + buildId + "/downloads/elasticsearch/[module]-[revision].[ext]");
                    } else {
                        p.artifact("/elasticsearch/" + buildId + "/downloads/elasticsearch/[module]-[revision]-[classifier].[ext]");
                    }
                });
                repo.metadataSources(s -> s.artifact());
                repo.content(c -> c.includeVersionByRegex(effectiveMavenGroup, effectiveMavenModule, ".*-SNAPSHOT"));
            });
        }

        // Resolvable configuration: requests directory type for archives that support expansion,
        // or the raw artifact type for packages (deb, rpm) and older archives.
        String draConfigName = "draResolvable-" + bwcVersion.get() + "-" + projectName;
        Configuration draConfig = project.getConfigurations().create(draConfigName);
        draConfig.setCanBeResolved(true);
        draConfig.setCanBeConsumed(false);
        draConfig.getAttributes().attribute(BWC_DISTRIBUTION_ATTRIBUTE, true);
        if (useNativeExpanded) {
            draConfig.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
        } else {
            draConfig.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, extension);
        }

        String versionString = bwcVersion.get() + "-SNAPSHOT";
        String dependencyNotation = gradleClassifier.isEmpty()
            ? effectiveMavenGroup + ":" + effectiveMavenModule + ":" + versionString + "@" + extension
            : effectiveMavenGroup + ":" + effectiveMavenModule + ":" + versionString + ":" + gradleClassifier + "@" + extension;
        project.getDependencies().add(draConfigName, dependencyNotation);

        File rootDir = project.getLayout().getSettingsDirectory().getAsFile();
        if (mavenModule.isEmpty() == false) {
            // Maven JAR artifacts (JDBC, stable API): use a dedicated task type so that
            // getOutputs().getFiles() contains exactly the downloaded JAR. A Copy task's
            // implicit @OutputDirectory would also appear in the output file set, causing
            // JarApiComparisonTask's single-jar assertion to fail with both the JAR name
            // and the parent directory name ("distributions") in the set.
            // A typed task class also avoids Configuration Cache issues: a plain
            // DefaultTask doLast lambda cannot capture a Configuration object.
            project.getTasks().register(bwcTaskName, DownloadMavenJarTask.class, task -> {
                task.doFirst(
                    t -> t.getLogger()
                        .lifecycle(
                            "BWC [{}]: downloading pre-built [{}] from DRA snapshot {} — skipping source build",
                            bwcVersion.get(),
                            projectName,
                            buildId
                        )
                );
                task.getDraJar().from(draConfig);
                task.getDraBuildId().set(buildId);
                task.getOutputJar().set(projectArtifact.distFile);
                task.getOutputs().doNotCacheIf("BWC distribution caching is disabled for local builds", t -> buildParams.getCi() == false);
            });
        } else {
            // Distribution archives: use a Copy task which handles tar.gz/zip extraction
            // via registered artifact transforms.
            project.getTasks().register(bwcTaskName, Copy.class, t -> {
                t.doFirst(
                    task -> task.getLogger()
                        .lifecycle(
                            "BWC [{}]: downloading pre-built distribution [{}] from DRA snapshot {} — skipping source build",
                            bwcVersion.get(),
                            projectName,
                            buildId
                        )
                );
                t.from(draConfig);
                t.getInputs().property("draBuildId", buildId);
                if (useNativeExpanded) {
                    t.into(projectArtifact.expandedDistDir);
                    t.getOutputs().dir(expectedOutputFile);
                } else {
                    t.into(projectArtifact.distFile.getParentFile());
                    t.getOutputs().files(projectArtifact.distFile);
                }
                t.getOutputs().doNotCacheIf("BWC distribution caching is disabled for local builds", task -> buildParams.getCi() == false);
                t.doLast(task -> {
                    if (expectedOutputFile.exists() == false) {
                        Path relativeOutputPath = rootDir.toPath().relativize(expectedOutputFile.toPath());
                        throw new InvalidUserDataException(
                            "Downloading %s from DRA didn't produce expected artifact [%s].".formatted(bwcVersion.get(), relativeOutputPath)
                        );
                    }
                });
            });
        }
        bwcTaskProvider.configure(t -> t.dependsOn(bwcTaskName));
    }

    /**
     * Validates the {@code tests.bwc.mode} value, throwing {@link InvalidUserDataException} for
     * unrecognised values so users get a clear error message rather than a silent no-op.
     */
    static void validateBwcMode(String mode) {
        if (Set.of("gradle", "dra", "auto").contains(mode) == false) {
            throw new InvalidUserDataException("Invalid tests.bwc.mode value [" + mode + "]. Must be one of: gradle, dra, auto");
        }
    }

    /**
     * Returns the human-readable log message to emit when the gradle source-build fallback is
     * taken.  The message varies by mode (to explain <em>why</em> the fallback occurred) and by
     * artifact type (distribution archive vs. Maven JAR).
     *
     * <p>Extracted as a package-private static method so it can be exercised by unit tests without
     * standing up a full Gradle project.
     */
    static String buildFallbackMessage(String bwcMode, boolean isDistributionArchive, String bwcVersion, String projectName) {
        String draUnavailable = " — tests.bwc.mode=dra but no DRA snapshot was available"
            + " (endpoint unreachable or no build for this branch yet); falling back to source build";
        if (isDistributionArchive) {
            return switch (bwcMode) {
                case "dra" -> "BWC [" + bwcVersion + "]: building distribution [" + projectName + "] from source" + draUnavailable;
                case "auto" -> "BWC ["
                    + bwcVersion
                    + "]: building distribution ["
                    + projectName
                    + "] from source"
                    + " — DRA snapshot commit did not match local remote-tracking ref (or was unreachable)";
                default -> "BWC ["
                    + bwcVersion
                    + "]: building distribution ["
                    + projectName
                    + "] from source"
                    + " — set -Dtests.bwc.mode=auto to use a pre-built DRA snapshot when the commit matches";
            };
        } else {
            return switch (bwcMode) {
                case "dra" -> "BWC [" + bwcVersion + "]: building [" + projectName + "] from source" + draUnavailable;
                default -> "BWC [" + bwcVersion + "]: building [" + projectName + "] from source";
            };
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
     * Downloads a single Maven JAR artifact from the DRA snapshot repository into the declared
     * output file location.
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
