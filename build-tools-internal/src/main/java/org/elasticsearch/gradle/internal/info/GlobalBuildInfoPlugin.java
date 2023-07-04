/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.info;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.gradle.internal.BwcVersions;
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
import org.gradle.jvm.toolchain.JavaLauncher;
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
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

public class GlobalBuildInfoPlugin implements Plugin<Project> {
    private static final Logger LOGGER = Logging.getLogger(GlobalBuildInfoPlugin.class);
    private static final String DEFAULT_VERSION_JAVA_FILE_PATH = "server/src/main/java/org/elasticsearch/Version.java";

    private ObjectFactory objectFactory;
    private final JavaInstallationRegistry javaInstallationRegistry;
    private final JvmMetadataDetector metadataDetector;
    private final ProviderFactory providers;
    private JavaToolchainService toolChainService;

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

    }

    @Override
    public void apply(Project project) {
        if (project != project.getRootProject()) {
            throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
        }
        project.getPlugins().apply(JvmToolchainsPlugin.class);
        toolChainService = project.getExtensions().getByType(JavaToolchainService.class);
        GradleVersion minimumGradleVersion = GradleVersion.version(getResourceContents("/minimumGradleVersion"));
        if (GradleVersion.current().compareTo(minimumGradleVersion) < 0) {
            throw new GradleException("Gradle " + minimumGradleVersion.getVersion() + "+ is required");
        }

        JavaVersion minimumCompilerVersion = JavaVersion.toVersion(getResourceContents("/minimumCompilerVersion"));
        JavaVersion minimumRuntimeVersion = JavaVersion.toVersion(getResourceContents("/minimumRuntimeVersion"));

        File runtimeJavaHome = findRuntimeJavaHome();
        boolean isRuntimeJavaHomeSet = Jvm.current().getJavaHome().equals(runtimeJavaHome) == false;

        GitInfo gitInfo = GitInfo.gitInfo(project.getRootDir());

        BuildParams.init(params -> {
            params.reset();
            params.setRuntimeJavaHome(runtimeJavaHome);
            params.setJavaToolChainSpec(resolveToolchainSpecFromEnv());
            params.setRuntimeJavaVersion(
                determineJavaVersion(
                    "runtime java.home",
                    runtimeJavaHome,
                    isRuntimeJavaHomeSet ? minimumRuntimeVersion : Jvm.current().getJavaVersion()
                )
            );
            params.setIsRuntimeJavaHomeSet(isRuntimeJavaHomeSet);
            JvmInstallationMetadata runtimeJdkMetaData = metadataDetector.getMetadata(getJavaInstallation(runtimeJavaHome));
            params.setRuntimeJavaDetails(formatJavaVendorDetails(runtimeJdkMetaData));
            params.setJavaVersions(getAvailableJavaVersions());
            params.setMinimumCompilerVersion(minimumCompilerVersion);
            params.setMinimumRuntimeVersion(minimumRuntimeVersion);
            params.setGradleJavaVersion(Jvm.current().getJavaVersion());
            params.setGitRevision(gitInfo.getRevision());
            params.setGitOrigin(gitInfo.getOrigin());
            params.setBuildDate(ZonedDateTime.now(ZoneOffset.UTC));
            params.setTestSeed(getTestSeed());
            params.setIsCi(System.getenv("JENKINS_URL") != null || System.getenv("BUILDKITE_BUILD_URL") != null);
            params.setDefaultParallel(ParallelDetector.findDefaultParallel(project));
            params.setInFipsJvm(Util.getBooleanProperty("tests.fips.enabled", false));
            params.setIsSnapshotBuild(Util.getBooleanProperty("build.snapshot", true));
            AtomicReference<BwcVersions> cache = new AtomicReference<>();
            params.setBwcVersions(
                providers.provider(
                    () -> cache.updateAndGet(
                        val -> val == null ? resolveBwcVersions(Util.locateElasticsearchWorkspace(project.getGradle())) : val
                    )
                )
            );
        });

        // Enforce the minimum compiler version
        assertMinimumCompilerVersion(minimumCompilerVersion);

        // Print global build info header just before task execution
        // Only do this if we are the root build of a composite
        if (GradleUtils.isIncludedBuild(project) == false) {
            project.getGradle().getTaskGraph().whenReady(graph -> logGlobalBuildInfo());
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
    private static BwcVersions resolveBwcVersions(File root) {
        File versionsFile = new File(root, DEFAULT_VERSION_JAVA_FILE_PATH);
        try {
            List<String> versionLines = IOUtils.readLines(new FileInputStream(versionsFile), "UTF-8");
            return new BwcVersions(versionLines);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to resolve to resolve bwc versions from versionsFile.", e);
        }
    }

    private void logGlobalBuildInfo() {
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
        if (BuildParams.getIsRuntimeJavaHomeSet()) {
            JvmInstallationMetadata runtimeJvm = metadataDetector.getMetadata(getJavaInstallation(BuildParams.getRuntimeJavaHome()));
            final String runtimeJvmVendorDetails = runtimeJvm.getVendor().getDisplayName();
            final String runtimeJvmImplementationVersion = runtimeJvm.getJvmVersion();
            final String runtimeVersion = runtimeJvm.getRuntimeVersion();
            final String runtimeExtraDetails = runtimeJvmVendorDetails + ", " + runtimeVersion;
            LOGGER.quiet("  Runtime JDK Version   : " + runtimeJvmImplementationVersion + " (" + runtimeExtraDetails + ")");
            LOGGER.quiet("  Runtime java.home     : " + BuildParams.getRuntimeJavaHome());
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
        LOGGER.quiet("  Random Testing Seed   : " + BuildParams.getTestSeed());
        LOGGER.quiet("  In FIPS 140 mode      : " + BuildParams.isInFipsJvm());
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
            Stream.of(new InstallationLocation(Jvm.current().getJavaHome(), "Current JVM"))
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

    private File findRuntimeJavaHome() {
        String runtimeJavaProperty = System.getProperty("runtime.java");

        if (runtimeJavaProperty != null) {
            return new File(findJavaHome(runtimeJavaProperty));
        }
        String env = System.getenv("RUNTIME_JAVA_HOME");
        if (env != null) {
            return new File(env);
        }
        // fall back to tool chain if set.
        env = System.getenv("JAVA_TOOLCHAIN_HOME");
        return env == null ? Jvm.current().getJavaHome() : new File(env);
    }

    private String findJavaHome(String version) {
        String javaHomeEnvVar = getJavaHomeEnvVarName(version);
        String env = System.getenv(javaHomeEnvVar);
        return env != null ? resolveJavaHomeFromEnvVariable(javaHomeEnvVar) : resolveJavaHomeFromToolChainService(version);
    }

    @NotNull
    private String resolveJavaHomeFromEnvVariable(String javaHomeEnvVar) {
        Provider<String> javaHomeNames = providers.gradleProperty("org.gradle.java.installations.fromEnv");
        // Provide a useful error if we're looking for a Java home version that we haven't told Gradle about yet
        Arrays.stream(javaHomeNames.get().split(","))
            .filter(s -> s.equals(javaHomeEnvVar))
            .findFirst()
            .orElseThrow(
                () -> new GradleException(
                    "Environment variable '"
                        + javaHomeEnvVar
                        + "' is not registered with Gradle installation supplier. Ensure 'org.gradle.java.installations.fromEnv' is "
                        + "updated in gradle.properties file."
                )
            );
        String versionedJavaHome = System.getenv(javaHomeEnvVar);
        if (versionedJavaHome == null) {
            final String exceptionMessage = String.format(
                Locale.ROOT,
                "$%s must be set to build Elasticsearch. "
                    + "Note that if the variable was just set you "
                    + "might have to run `./gradlew --stop` for "
                    + "it to be picked up. See https://github.com/elastic/elasticsearch/issues/31399 details.",
                javaHomeEnvVar
            );
            throw new GradleException(exceptionMessage);
        }
        return versionedJavaHome;
    }

    @NotNull
    private String resolveJavaHomeFromToolChainService(String version) {
        Property<JavaLanguageVersion> value = objectFactory.property(JavaLanguageVersion.class).value(JavaLanguageVersion.of(version));
        Provider<JavaLauncher> javaLauncherProvider = toolChainService.launcherFor(
            javaToolchainSpec -> javaToolchainSpec.getLanguageVersion().value(value)
        );

        try {
            return javaLauncherProvider.get().getMetadata().getInstallationPath().getAsFile().getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getJavaHomeEnvVarName(String version) {
        return "JAVA" + version + "_HOME";
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
