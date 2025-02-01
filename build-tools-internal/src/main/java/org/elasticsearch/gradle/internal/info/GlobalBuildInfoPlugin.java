/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.info;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.BwcVersions;
import org.elasticsearch.gradle.internal.conventions.GitInfoPlugin;
import org.elasticsearch.gradle.internal.conventions.info.GitInfo;
import org.elasticsearch.gradle.internal.conventions.info.ParallelDetector;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.plugins.JvmToolchainsPlugin;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.internal.jvm.Jvm;
import org.gradle.internal.jvm.inspection.JavaInstallationRegistry;
import org.gradle.internal.jvm.inspection.JvmInstallationMetadata;
import org.gradle.internal.jvm.inspection.JvmMetadataDetector;
import org.gradle.internal.jvm.inspection.JvmVendor;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;
import org.gradle.jvm.toolchain.JavaToolchainSpec;
import org.gradle.jvm.toolchain.JvmVendorSpec;
import org.gradle.jvm.toolchain.internal.InstallationLocation;
import org.gradle.util.GradleVersion;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.conventions.GUtils.elvis;

public class GlobalBuildInfoPlugin implements Plugin<Project> {
    private static final Logger LOGGER = Logging.getLogger(GlobalBuildInfoPlugin.class);
    private static final String DEFAULT_VERSION_JAVA_FILE_PATH = "server/src/main/java/org/elasticsearch/Version.java";

    private ObjectFactory objectFactory;
    private final JavaInstallationRegistry javaInstallationRegistry;
    private final JvmMetadataDetector metadataDetector;
    private final ProviderFactory providers;
    private final ObjectMapper objectMapper;
    private JavaToolchainService toolChainService;
    private Project project;

    @Inject
    public GlobalBuildInfoPlugin(
        ObjectFactory objectFactory,
        JavaInstallationRegistry javaInstallationRegistry,
        JvmMetadataDetector metadataDetector,
        ProviderFactory providers
    ) {
        this.objectFactory = objectFactory;
        this.javaInstallationRegistry = javaInstallationRegistry;
        this.metadataDetector = new ErrorTraceMetadataDetector(metadataDetector);
        this.providers = providers;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void apply(Project project) {
        if (project != project.getRootProject()) {
            throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
        }
        this.project = project;
        project.getPlugins().apply(JvmToolchainsPlugin.class);
        Provider<GitInfo> gitInfo = project.getPlugins().apply(GitInfoPlugin.class).getGitInfo();

        toolChainService = project.getExtensions().getByType(JavaToolchainService.class);
        GradleVersion minimumGradleVersion = GradleVersion.version(getResourceContents("/minimumGradleVersion"));
        if (GradleVersion.current().compareTo(minimumGradleVersion) < 0) {
            throw new GradleException("Gradle " + minimumGradleVersion.getVersion() + "+ is required");
        }

        JavaVersion minimumCompilerVersion = JavaVersion.toVersion(getResourceContents("/minimumCompilerVersion"));
        JavaVersion minimumRuntimeVersion = JavaVersion.toVersion(getResourceContents("/minimumRuntimeVersion"));

        Provider<File> explicitRuntimeJavaHome = findRuntimeJavaHome();
        boolean isRuntimeJavaHomeExplicitlySet = explicitRuntimeJavaHome.isPresent();
        Provider<File> actualRuntimeJavaHome = isRuntimeJavaHomeExplicitlySet
            ? explicitRuntimeJavaHome
            : resolveJavaHomeFromToolChainService(VersionProperties.getBundledJdkMajorVersion());

        Provider<JvmInstallationMetadata> runtimeJdkMetaData = actualRuntimeJavaHome.map(
            runtimeJavaHome -> metadataDetector.getMetadata(getJavaInstallation(runtimeJavaHome))
        );
        AtomicReference<BwcVersions> cache = new AtomicReference<>();
        Provider<BwcVersions> bwcVersionsProvider = providers.provider(
            () -> cache.updateAndGet(val -> val == null ? resolveBwcVersions() : val)
        );
        BuildParameterExtension buildParams = project.getExtensions()
            .create(
                BuildParameterExtension.class,
                BuildParameterExtension.EXTENSION_NAME,
                DefaultBuildParameterExtension.class,
                providers,
                actualRuntimeJavaHome,
                resolveToolchainSpecFromEnv(),
                actualRuntimeJavaHome.map(
                    javaHome -> determineJavaVersion(
                        "runtime java.home",
                        javaHome,
                        isRuntimeJavaHomeExplicitlySet
                            ? minimumRuntimeVersion
                            : JavaVersion.toVersion(VersionProperties.getBundledJdkMajorVersion())
                    )
                ),
                isRuntimeJavaHomeExplicitlySet,
                runtimeJdkMetaData.map(m -> formatJavaVendorDetails(m)),
                getAvailableJavaVersions(),
                minimumCompilerVersion,
                minimumRuntimeVersion,
                Jvm.current().getJavaVersion(),
                gitInfo.map(g -> g.getRevision()),
                gitInfo.map(g -> g.getOrigin()),
                getTestSeed(),
                System.getenv("JENKINS_URL") != null || System.getenv("BUILDKITE_BUILD_URL") != null || System.getProperty("isCI") != null,
                ParallelDetector.findDefaultParallel(project),
                Util.getBooleanProperty("build.snapshot", true),
                bwcVersionsProvider
            );

        project.getGradle().getSharedServices().registerIfAbsent("buildParams", BuildParameterService.class, spec -> {
            // Provide some parameters
            spec.getParameters().getBuildParams().set(buildParams);
        });

        // Enforce the minimum compiler version
        assertMinimumCompilerVersion(minimumCompilerVersion);

        // Print global build info header just before task execution
        // Only do this if we are the root build of a composite
        if (GradleUtils.isIncludedBuild(project) == false) {
            project.getGradle().getTaskGraph().whenReady(graph -> logGlobalBuildInfo(buildParams));
        }
    }

    private Provider<MetadataBasedToolChainMatcher> resolveToolchainSpecFromEnv() {
        return providers.environmentVariable("JAVA_TOOLCHAIN_HOME").map(toolChainEnvVariable -> {
            File toolChainDir = new File(toolChainEnvVariable);
            JvmInstallationMetadata metadata = metadataDetector.getMetadata(getJavaInstallation(toolChainDir));
            if (metadata.isValidInstallation() == false) {
                throw new GradleException(
                    "Configured JAVA_TOOLCHAIN_HOME " + toolChainEnvVariable + " does not point to a valid jdk installation."
                );
            }
            return new MetadataBasedToolChainMatcher(metadata);
        });
    }

    private String formatJavaVendorDetails(JvmInstallationMetadata runtimeJdkMetaData) {
        JvmVendor vendor = runtimeJdkMetaData.getVendor();
        return runtimeJdkMetaData.getVendor().getKnownVendor().name() + "/" + vendor.getRawVendor();
    }

    /* Introspect all versions of ES that may be tested against for backwards
     * compatibility. It is *super* important that this logic is the same as the
     * logic in VersionUtils.java. */
    private BwcVersions resolveBwcVersions() {
        String versionsFilePath = elvis(
            System.getProperty("BWC_VERSION_SOURCE"),
            new File(Util.locateElasticsearchWorkspace(project.getGradle()), DEFAULT_VERSION_JAVA_FILE_PATH).getPath()
        );
        try (var is = new FileInputStream(versionsFilePath)) {
            List<String> versionLines = IOUtils.readLines(is, "UTF-8");
            return new BwcVersions(versionLines, getDevelopmentBranches());
        } catch (IOException e) {
            throw new IllegalStateException("Unable to resolve to resolve bwc versions from versionsFile.", e);
        }
    }

    private List<String> getDevelopmentBranches() {
        List<String> branches = new ArrayList<>();
        File branchesFile = new File(Util.locateElasticsearchWorkspace(project.getGradle()), "branches.json");
        try (InputStream is = new FileInputStream(branchesFile)) {
            JsonNode json = objectMapper.readTree(is);
            for (JsonNode node : json.get("branches")) {
                branches.add(node.get("branch").asText());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return branches;
    }

    private void logGlobalBuildInfo(BuildParameterExtension buildParams) {
        final String osName = System.getProperty("os.name");
        final String osVersion = System.getProperty("os.version");
        final String osArch = System.getProperty("os.arch");
        final Jvm gradleJvm = Jvm.current();
        JvmInstallationMetadata gradleJvmMetadata = metadataDetector.getMetadata(getJavaInstallation(gradleJvm.getJavaHome()));
        final String gradleJvmVendorDetails = gradleJvmMetadata.getVendor().getDisplayName();
        final String gradleJvmImplementationVersion = gradleJvmMetadata.getJvmVersion();
        LOGGER.quiet("=======================================");
        LOGGER.quiet("Elasticsearch Build Hamster says Hello!");
        LOGGER.quiet("  Gradle Version        : " + GradleVersion.current().getVersion());
        LOGGER.quiet("  OS Info               : " + osName + " " + osVersion + " (" + osArch + ")");
        if (buildParams.getIsRuntimeJavaHomeSet()) {
            JvmInstallationMetadata runtimeJvm = metadataDetector.getMetadata(getJavaInstallation(buildParams.getRuntimeJavaHome().get()));
            final String runtimeJvmVendorDetails = runtimeJvm.getVendor().getDisplayName();
            final String runtimeJvmImplementationVersion = runtimeJvm.getJvmVersion();
            final String runtimeVersion = runtimeJvm.getRuntimeVersion();
            final String runtimeExtraDetails = runtimeJvmVendorDetails + ", " + runtimeVersion;
            LOGGER.quiet("  Runtime JDK Version   : " + runtimeJvmImplementationVersion + " (" + runtimeExtraDetails + ")");
            LOGGER.quiet("  Runtime java.home     : " + buildParams.getRuntimeJavaHome().get());
            LOGGER.quiet("  Gradle JDK Version    : " + gradleJvmImplementationVersion + " (" + gradleJvmVendorDetails + ")");
            LOGGER.quiet("  Gradle java.home      : " + gradleJvm.getJavaHome());
        } else {
            LOGGER.quiet("  JDK Version           : " + gradleJvmImplementationVersion + " (" + gradleJvmVendorDetails + ")");
            LOGGER.quiet("  JAVA_HOME             : " + gradleJvm.getJavaHome());
        }
        String javaToolchainHome = System.getenv("JAVA_TOOLCHAIN_HOME");
        if (javaToolchainHome != null) {
            LOGGER.quiet("  JAVA_TOOLCHAIN_HOME   : " + javaToolchainHome);
        }
        LOGGER.quiet("  Random Testing Seed   : " + buildParams.getTestSeed());
        LOGGER.quiet("  In FIPS 140 mode      : " + buildParams.getInFipsJvm());
        LOGGER.quiet("=======================================");
    }

    private JavaVersion determineJavaVersion(String description, File javaHome, JavaVersion requiredVersion) {
        InstallationLocation installation = getJavaInstallation(javaHome);
        JavaVersion actualVersion = metadataDetector.getMetadata(installation).getLanguageVersion();
        if (actualVersion.isCompatibleWith(requiredVersion) == false) {
            throwInvalidJavaHomeException(
                description,
                javaHome,
                Integer.parseInt(requiredVersion.getMajorVersion()),
                Integer.parseInt(actualVersion.getMajorVersion())
            );
        }

        return actualVersion;
    }

    private InstallationLocation getJavaInstallation(File javaHome) {
        return getAvailableJavaInstallationLocationSteam().filter(installationLocation -> isSameFile(javaHome, installationLocation))
            .findFirst()
            .orElseThrow(() -> new GradleException("Could not locate available Java installation in Gradle registry at: " + javaHome));
    }

    private boolean isSameFile(File javaHome, InstallationLocation installationLocation) {
        try {
            return Files.isSameFile(javaHome.toPath(), installationLocation.getLocation().toPath());
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
    }

    /**
     * We resolve all available java versions using auto detected by gradles tool chain
     * To make transition more reliable we only take env var provided installations into account for now
     */
    private List<JavaHome> getAvailableJavaVersions() {
        return getAvailableJavaInstallationLocationSteam().map(installationLocation -> {
            JvmInstallationMetadata metadata = metadataDetector.getMetadata(installationLocation);
            int actualVersion = Integer.parseInt(metadata.getLanguageVersion().getMajorVersion());
            return JavaHome.of(actualVersion, providers.provider(() -> installationLocation.getLocation()));
        }).collect(Collectors.toList());
    }

    private Stream<InstallationLocation> getAvailableJavaInstallationLocationSteam() {
        return Stream.concat(
            javaInstallationRegistry.toolchains().stream().map(metadata -> metadata.location),
            Stream.of(InstallationLocation.userDefined(Jvm.current().getJavaHome(), "Current JVM"))
        );
    }

    private static String getTestSeed() {
        String testSeedProperty = System.getProperty("tests.seed");
        final String testSeed;
        if (testSeedProperty == null) {
            long seed = new Random(System.currentTimeMillis()).nextLong();
            testSeed = Long.toUnsignedString(seed, 16).toUpperCase(Locale.ROOT);
        } else {
            testSeed = testSeedProperty;
        }
        return testSeed;
    }

    private static void throwInvalidJavaHomeException(String description, File javaHome, int expectedVersion, int actualVersion) {
        String message = String.format(
            Locale.ROOT,
            "The %s must be set to a JDK installation directory for Java %d but is [%s] corresponding to [%s]",
            description,
            expectedVersion,
            javaHome,
            actualVersion
        );

        throw new GradleException(message);
    }

    private static void assertMinimumCompilerVersion(JavaVersion minimumCompilerVersion) {
        JavaVersion currentVersion = Jvm.current().getJavaVersion();
        if (System.getProperty("idea.active", "false").equals("true") == false && minimumCompilerVersion.compareTo(currentVersion) > 0) {
            throw new GradleException(
                "Project requires Java version of " + minimumCompilerVersion + " or newer but Gradle JAVA_HOME is " + currentVersion
            );
        }
    }

    private Provider<File> findRuntimeJavaHome() {
        String runtimeJavaProperty = System.getProperty("runtime.java");

        if (runtimeJavaProperty != null) {
            return resolveJavaHomeFromToolChainService(runtimeJavaProperty);
        }
        if (System.getenv("RUNTIME_JAVA_HOME") != null) {
            return providers.provider(() -> new File(System.getenv("RUNTIME_JAVA_HOME")));
        }
        // fall back to tool chain if set.
        String env = System.getenv("JAVA_TOOLCHAIN_HOME");
        return providers.provider(() -> {
            if (env == null) {
                return null;
            }
            return new File(env);
        });
    }

    @NotNull
    private Provider<File> resolveJavaHomeFromToolChainService(String version) {
        Property<JavaLanguageVersion> value = objectFactory.property(JavaLanguageVersion.class).value(JavaLanguageVersion.of(version));
        return toolChainService.launcherFor(javaToolchainSpec -> javaToolchainSpec.getLanguageVersion().value(value))
            .map(launcher -> launcher.getMetadata().getInstallationPath().getAsFile());
    }

    public static String getResourceContents(String resourcePath) {
        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(GlobalBuildInfoPlugin.class.getResourceAsStream(resourcePath)))
        ) {
            StringBuilder b = new StringBuilder();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (b.length() != 0) {
                    b.append('\n');
                }
                b.append(line);
            }

            return b.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("Error trying to read classpath resource: " + resourcePath, e);
        }
    }

    private static class ErrorTraceMetadataDetector implements JvmMetadataDetector {
        private final JvmMetadataDetector delegate;

        ErrorTraceMetadataDetector(JvmMetadataDetector delegate) {
            this.delegate = delegate;
        }

        @Override
        public JvmInstallationMetadata getMetadata(InstallationLocation installationLocation) {
            JvmInstallationMetadata metadata = delegate.getMetadata(installationLocation);
            if (metadata instanceof JvmInstallationMetadata.FailureInstallationMetadata) {
                throw new GradleException("Jvm Metadata cannot be resolved for " + metadata.getJavaHome().toString());
            }
            return metadata;
        }
    }

    private static class MetadataBasedToolChainMatcher implements Action<JavaToolchainSpec> {
        private final JvmVendorSpec expectedVendorSpec;
        private final JavaLanguageVersion expectedJavaLanguageVersion;

        MetadataBasedToolChainMatcher(JvmInstallationMetadata metadata) {
            expectedVendorSpec = JvmVendorSpec.matching(metadata.getVendor().getRawVendor());
            expectedJavaLanguageVersion = JavaLanguageVersion.of(metadata.getLanguageVersion().getMajorVersion());
        }

        @Override
        public void execute(JavaToolchainSpec spec) {
            spec.getVendor().set(expectedVendorSpec);
            spec.getLanguageVersion().set(expectedJavaLanguageVersion);
        }
    }
}
