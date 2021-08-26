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
import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.internal.conventions.info.GitInfo;
import org.elasticsearch.gradle.internal.conventions.info.ParallelDetector;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.internal.jvm.Jvm;
import org.gradle.internal.jvm.inspection.JvmInstallationMetadata;
import org.gradle.internal.jvm.inspection.JvmMetadataDetector;
import org.gradle.internal.jvm.inspection.JvmVendor;
import org.gradle.jvm.toolchain.internal.InstallationLocation;
import org.gradle.jvm.toolchain.internal.JavaInstallationRegistry;
import org.gradle.util.GradleVersion;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GlobalBuildInfoPlugin implements Plugin<Project> {
    private static final Logger LOGGER = Logging.getLogger(GlobalBuildInfoPlugin.class);
    private static final String DEFAULT_VERSION_JAVA_FILE_PATH = "server/src/main/java/org/elasticsearch/Version.java";
    private static Integer _defaultParallel = null;

    private final JavaInstallationRegistry javaInstallationRegistry;
    private final JvmMetadataDetector metadataDetector;
    private final ProviderFactory providers;

    @Inject
    public GlobalBuildInfoPlugin(
        JavaInstallationRegistry javaInstallationRegistry,
        JvmMetadataDetector metadataDetector,
        ProviderFactory providers
    ) {
        this.javaInstallationRegistry = javaInstallationRegistry;
        this.metadataDetector = new ErrorTraceMetadataDetector(metadataDetector);
        this.providers = providers;
    }

    @Override
    public void apply(Project project) {
        if (project != project.getRootProject()) {
            throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
        }
        GradleVersion minimumGradleVersion = GradleVersion.version(getResourceContents("/minimumGradleVersion"));
        if (GradleVersion.current().compareTo(minimumGradleVersion) < 0) {
            throw new GradleException("Gradle " + minimumGradleVersion.getVersion() + "+ is required");
        }

        JavaVersion minimumCompilerVersion = JavaVersion.toVersion(getResourceContents("/minimumCompilerVersion"));
        JavaVersion minimumRuntimeVersion = JavaVersion.toVersion(getResourceContents("/minimumRuntimeVersion"));

        File runtimeJavaHome = findRuntimeJavaHome();

        File rootDir = project.getRootDir();
        GitInfo gitInfo = GitInfo.gitInfo(rootDir);

        BuildParams.init(params -> {
            params.reset();
            params.setRuntimeJavaHome(runtimeJavaHome);
            params.setRuntimeJavaVersion(determineJavaVersion("runtime java.home", runtimeJavaHome, minimumRuntimeVersion));
            params.setIsRuntimeJavaHomeSet(Jvm.current().getJavaHome().equals(runtimeJavaHome) == false);
            JvmInstallationMetadata runtimeJdkMetaData = metadataDetector.getMetadata(getJavaInstallation(runtimeJavaHome).getLocation());
            params.setRuntimeJavaDetails(formatJavaVendorDetails(runtimeJdkMetaData));
            params.setJavaVersions(getAvailableJavaVersions());
            params.setMinimumCompilerVersion(minimumCompilerVersion);
            params.setMinimumRuntimeVersion(minimumRuntimeVersion);
            params.setGradleJavaVersion(Jvm.current().getJavaVersion());
            params.setGitRevision(gitInfo.getRevision());
            params.setGitOrigin(gitInfo.getOrigin());
            params.setBuildDate(ZonedDateTime.now(ZoneOffset.UTC));
            params.setTestSeed(getTestSeed());
            params.setIsCi(System.getenv("JENKINS_URL") != null);
            params.setDefaultParallel(ParallelDetector.findDefaultParallel(project));
            params.setInFipsJvm(Util.getBooleanProperty("tests.fips.enabled", false));
            params.setIsSnapshotBuild(Util.getBooleanProperty("build.snapshot", true));
            params.setBwcVersions(providers.provider(() -> resolveBwcVersions(rootDir)));
        });

        // Enforce the minimum compiler version
        assertMinimumCompilerVersion(minimumCompilerVersion);

        // Print global build info header just before task execution
        project.getGradle().getTaskGraph().whenReady(graph -> logGlobalBuildInfo());
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
        JvmInstallationMetadata gradleJvmMetadata = metadataDetector.getMetadata(gradleJvm.getJavaHome());
        final String gradleJvmVendorDetails = gradleJvmMetadata.getVendor().getDisplayName();
        final String gradleJvmImplementationVersion = gradleJvmMetadata.getImplementationVersion();
        LOGGER.quiet("=======================================");
        LOGGER.quiet("Elasticsearch Build Hamster says Hello!");
        LOGGER.quiet("  Gradle Version        : " + GradleVersion.current().getVersion());
        LOGGER.quiet("  OS Info               : " + osName + " " + osVersion + " (" + osArch + ")");
        if (BuildParams.getIsRuntimeJavaHomeSet()) {
            JvmInstallationMetadata runtimeJvm = metadataDetector.getMetadata(BuildParams.getRuntimeJavaHome());
            final String runtimeJvmVendorDetails = runtimeJvm.getVendor().getDisplayName();
            final String runtimeJvmImplementationVersion = runtimeJvm.getImplementationVersion();
            LOGGER.quiet("  Runtime JDK Version   : " + runtimeJvmImplementationVersion + " (" + runtimeJvmVendorDetails + ")");
            LOGGER.quiet("  Runtime java.home     : " + BuildParams.getRuntimeJavaHome());
            LOGGER.quiet("  Gradle JDK Version    : " + gradleJvmImplementationVersion + " (" + gradleJvmVendorDetails + ")");
            LOGGER.quiet("  Gradle java.home      : " + gradleJvm.getJavaHome());
        } else {
            LOGGER.quiet("  JDK Version           : " + gradleJvmImplementationVersion + " (" + gradleJvmVendorDetails + ")");
            LOGGER.quiet("  JAVA_HOME             : " + gradleJvm.getJavaHome());
        }
        LOGGER.quiet("  Random Testing Seed   : " + BuildParams.getTestSeed());
        LOGGER.quiet("  In FIPS 140 mode      : " + BuildParams.isInFipsJvm());
        LOGGER.quiet("=======================================");
    }

    private JavaVersion determineJavaVersion(String description, File javaHome, JavaVersion requiredVersion) {
        InstallationLocation installation = getJavaInstallation(javaHome);
        JavaVersion actualVersion = metadataDetector.getMetadata(installation.getLocation()).getLanguageVersion();
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
            return Files.isSameFile(installationLocation.getLocation().toPath(), javaHome.toPath());
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
            File installationDir = installationLocation.getLocation();
            JvmInstallationMetadata metadata = metadataDetector.getMetadata(installationDir);
            int actualVersion = Integer.parseInt(metadata.getLanguageVersion().getMajorVersion());
            return JavaHome.of(actualVersion, providers.provider(() -> installationDir));
        }).collect(Collectors.toList());
    }

    private Stream<InstallationLocation> getAvailableJavaInstallationLocationSteam() {
        return Stream.concat(
            javaInstallationRegistry.listInstallations().stream(),
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

        return System.getenv("RUNTIME_JAVA_HOME") == null ? Jvm.current().getJavaHome() : new File(System.getenv("RUNTIME_JAVA_HOME"));
    }

    private String findJavaHome(String version) {
        Provider<String> javaHomeNames = providers.gradleProperty("org.gradle.java.installations.fromEnv").forUseAtConfigurationTime();
        String javaHomeEnvVar = getJavaHomeEnvVarName(version);

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

    private static String getJavaHomeEnvVarName(String version) {
        return "JAVA" + version + "_HOME";
    }

    private static int findDefaultParallel(Project project) {
        // Since it costs IO to compute this, and is done at configuration time we want to cache this if possible
        // It's safe to store this in a static variable since it's just a primitive so leaking memory isn't an issue
        if (_defaultParallel == null) {
            File cpuInfoFile = new File("/proc/cpuinfo");
            if (cpuInfoFile.exists()) {
                // Count physical cores on any Linux distro ( don't count hyper-threading )
                Map<String, Integer> socketToCore = new HashMap<>();
                String currentID = "";

                try (BufferedReader reader = new BufferedReader(new FileReader(cpuInfoFile))) {
                    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                        if (line.contains(":")) {
                            List<String> parts = Arrays.stream(line.split(":", 2)).map(String::trim).collect(Collectors.toList());
                            String name = parts.get(0);
                            String value = parts.get(1);
                            // the ID of the CPU socket
                            if (name.equals("physical id")) {
                                currentID = value;
                            }
                            // Number of cores not including hyper-threading
                            if (name.equals("cpu cores")) {
                                assert currentID.isEmpty() == false;
                                socketToCore.put("currentID", Integer.valueOf(value));
                                currentID = "";
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                _defaultParallel = socketToCore.values().stream().mapToInt(i -> i).sum();
            } else if (OS.current() == OS.MAC) {
                // Ask macOS to count physical CPUs for us
                ByteArrayOutputStream stdout = new ByteArrayOutputStream();
                project.exec(spec -> {
                    spec.setExecutable("sysctl");
                    spec.args("-n", "hw.physicalcpu");
                    spec.setStandardOutput(stdout);
                });

                _defaultParallel = Integer.parseInt(stdout.toString().trim());
            }

            _defaultParallel = Runtime.getRuntime().availableProcessors() / 2;
        }

        return _defaultParallel;
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
        public JvmInstallationMetadata getMetadata(File file) {
            JvmInstallationMetadata metadata = delegate.getMetadata(file);
            if(metadata instanceof JvmInstallationMetadata.FailureInstallationMetadata) {
                throw new GradleException("Jvm Metadata cannot be resolved for " + metadata.getJavaHome().toString());
            }
            return metadata;
        }
    }

}
