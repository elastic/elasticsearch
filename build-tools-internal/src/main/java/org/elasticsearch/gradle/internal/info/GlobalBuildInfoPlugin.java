/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.info;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.internal.BwcVersions;
import org.elasticsearch.gradle.internal.Jdk;
import org.elasticsearch.gradle.internal.JdkDownloadPlugin;
import org.elasticsearch.gradle.internal.conventions.GitInfoPlugin;
import org.elasticsearch.gradle.internal.conventions.VersionPropertiesPlugin;
import org.elasticsearch.gradle.internal.conventions.info.GitInfo;
import org.elasticsearch.gradle.internal.conventions.info.ParallelDetector;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.configuration.BuildFeatures;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.conventions.GUtils.elvis;
import static org.elasticsearch.gradle.internal.conventions.VersionPropertiesPlugin.VERSIONS_EXT;
import static org.elasticsearch.gradle.internal.toolchain.EarlyAccessCatalogJdkToolchainResolver.findLatestPreReleaseBuild;
import static org.elasticsearch.gradle.internal.toolchain.EarlyAccessCatalogJdkToolchainResolver.findPreReleaseBuild;

public class GlobalBuildInfoPlugin implements Plugin<Project> {
    private static final Logger LOGGER = Logging.getLogger(GlobalBuildInfoPlugin.class);
    private static final String DEFAULT_VERSION_JAVA_FILE_PATH = "server/src/main/java/org/elasticsearch/Version.java";
    private static final String DEFAULT_BRANCHES_FILE_URL = "https://raw.githubusercontent.com/elastic/elasticsearch/main/branches.json";
    private static final String BRANCHES_FILE_LOCATION_PROPERTY = "org.elasticsearch.build.branches-file-location";
    private static final Pattern LINE_PATTERN = Pattern.compile(
        "\\W+public static final Version V_(\\d+)_(\\d+)_(\\d+)(_alpha\\d+|_beta\\d+|_rc\\d+)?.*\\);"
    );

    private ObjectFactory objectFactory;
    private final JavaInstallationRegistry javaInstallationRegistry;
    private final JvmMetadataDetector metadataDetector;
    private final ProviderFactory providers;
    private final BuildFeatures buildFeatures;
    private JavaToolchainService toolChainService;
    private Project project;

    @Inject
    public GlobalBuildInfoPlugin(
        ObjectFactory objectFactory,
        JavaInstallationRegistry javaInstallationRegistry,
        JvmMetadataDetector metadataDetector,
        ProviderFactory providers,
        BuildFeatures buildFeatures
    ) {
        this.objectFactory = objectFactory;
        this.javaInstallationRegistry = javaInstallationRegistry;
        this.metadataDetector = new ErrorTraceMetadataDetector(metadataDetector);
        this.providers = providers;
        this.buildFeatures = buildFeatures;
    }

    @Override
    public void apply(Project project) {
        if (project != project.getRootProject()) {
            throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
        }
        this.project = project;
        project.getPlugins().apply(JvmToolchainsPlugin.class);
        project.getPlugins().apply(JdkDownloadPlugin.class);
        project.getPlugins().apply(VersionPropertiesPlugin.class);
        Provider<GitInfo> gitInfo = project.getPlugins().apply(GitInfoPlugin.class).getGitInfo();

        toolChainService = project.getExtensions().getByType(JavaToolchainService.class);
        GradleVersion minimumGradleVersion = GradleVersion.version(getResourceContents("/minimumGradleVersion"));
        if (GradleVersion.current().compareTo(minimumGradleVersion) < 0) {
            throw new GradleException("Gradle " + minimumGradleVersion.getVersion() + "+ is required");
        }

        Properties versionProperties = (Properties) project.getExtensions().getByName(VERSIONS_EXT);
        JavaVersion minimumCompilerVersion = JavaVersion.toVersion(versionProperties.get("minimumCompilerJava"));
        JavaVersion minimumRuntimeVersion = JavaVersion.toVersion(versionProperties.get("minimumRuntimeJava"));
        Version elasticsearchVersionProperty = Version.fromString(versionProperties.getProperty("elasticsearch"));

        RuntimeJava runtimeJavaHome = findRuntimeJavaHome();
        AtomicReference<BwcVersions> cache = new AtomicReference<>();
        Provider<BwcVersions> bwcVersionsProvider = providers.provider(
            () -> cache.updateAndGet(val -> val == null ? resolveBwcVersions(elasticsearchVersionProperty) : val)
        );

        BuildParameterExtension buildParams = project.getExtensions()
            .create(
                BuildParameterExtension.class,
                BuildParameterExtension.EXTENSION_NAME,
                DefaultBuildParameterExtension.class,
                providers,
                runtimeJavaHome,
                resolveToolchainSpecFromEnv(),
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
    private BwcVersions resolveBwcVersions(Version currentElasticsearchVersion) {
        String versionsFilePath = elvis(
            System.getProperty("BWC_VERSION_SOURCE"),
            new File(Util.locateElasticsearchWorkspace(project.getGradle()), DEFAULT_VERSION_JAVA_FILE_PATH).getPath()
        );
        try (var is = new FileInputStream(versionsFilePath)) {
            List<String> versionLines = IOUtils.readLines(is, "UTF-8");
            return new BwcVersions(currentElasticsearchVersion, parseVersionLines(versionLines), getDevelopmentBranches());
        } catch (IOException e) {
            throw new IllegalStateException("Unable to resolve to resolve bwc versions from versionsFile.", e);
        }
    }

    private List<Version> parseVersionLines(List<String> versionLines) {
        return versionLines.stream()
            .map(LINE_PATTERN::matcher)
            .filter(Matcher::matches)
            .map(match -> new Version(Integer.parseInt(match.group(1)), Integer.parseInt(match.group(2)), Integer.parseInt(match.group(3))))
            .sorted()
            .toList();
    }

    private List<DevelopmentBranch> getDevelopmentBranches() {
        String branchesFileLocation = project.getProviders()
            .gradleProperty(BRANCHES_FILE_LOCATION_PROPERTY)
            .getOrElse(DEFAULT_BRANCHES_FILE_URL);
        LOGGER.info("Reading branches.json from {}", branchesFileLocation);
        byte[] branchesBytes;
        if (branchesFileLocation.startsWith("http")) {
            try (InputStream in = URI.create(branchesFileLocation).toURL().openStream()) {
                branchesBytes = in.readAllBytes();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to download branches.json from: " + branchesFileLocation, e);
            }
        } else {
            try {
                branchesBytes = Files.readAllBytes(new File(branchesFileLocation).toPath());
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read branches.json from: " + branchesFileLocation, e);
            }
        }

        var branchesFileParser = new BranchesFileParser(new ObjectMapper());
        return branchesFileParser.parse(branchesBytes);
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
        if (buildParams.getRuntimeJava().isExplicitlySet()) {
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

        if (buildFeatures.getConfigurationCache().getActive().get() == false) {
            // if configuration cache is enabled, resolving the test seed early breaks configuration cache reuse
            LOGGER.quiet("  Random Testing Seed   : " + buildParams.getTestSeed());
        }
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
            .orElse(
                InstallationLocation.userDefined(javaHome, "Manually resolved JavaHome (not auto-detected by Gradle toolchain service)")
            );
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

    private Provider<String> getTestSeed() {
        return project.getProviders().of(TestSeedValueSource.class, spec -> {});
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

    private RuntimeJava findRuntimeJavaHome() {
        Properties versionProperties = (Properties) project.getExtensions().getByName(VERSIONS_EXT);
        String bundledJdkVersion = versionProperties.getProperty("bundled_jdk");
        String bundledJdkMajorVersion = bundledJdkVersion.split("[.+]")[0];

        String runtimeJavaProperty = System.getProperty("runtime.java");
        if (runtimeJavaProperty != null) {
            if (runtimeJavaProperty.toLowerCase().endsWith("-pre")) {
                // handle pre-release builds differently due to lack of support in Gradle toolchain service
                // we resolve them using JdkDownloadPlugin for now.
                return resolvePreReleaseRuntimeJavaHome(runtimeJavaProperty, bundledJdkMajorVersion);
            } else {
                return runtimeJavaHome(resolveJavaHomeFromToolChainService(runtimeJavaProperty), true, bundledJdkMajorVersion);
            }
        }
        if (System.getenv("RUNTIME_JAVA_HOME") != null) {
            return runtimeJavaHome(providers.provider(() -> new File(System.getenv("RUNTIME_JAVA_HOME"))), true, bundledJdkVersion);
        }
        // fall back to tool chain if set.
        String env = System.getenv("JAVA_TOOLCHAIN_HOME");
        boolean explicitlySet = env != null;
        Provider<File> javaHome = explicitlySet
            ? providers.provider(() -> new File(env))
            : resolveJavaHomeFromToolChainService(bundledJdkMajorVersion);
        return runtimeJavaHome(javaHome, explicitlySet, bundledJdkMajorVersion);
    }

    private RuntimeJava runtimeJavaHome(Provider<File> fileProvider, boolean explicitlySet, String bundledJdkMajorVersion) {
        return runtimeJavaHome(fileProvider, explicitlySet, null, null, bundledJdkMajorVersion);
    }

    private RuntimeJava runtimeJavaHome(
        Provider<File> fileProvider,
        boolean explicitlySet,
        String preReleasePostfix,
        Integer buildNumber,
        String bundledJdkMajorVersion
    ) {
        Provider<JavaVersion> javaVersion = fileProvider.map(
            javaHome -> determineJavaVersion(
                "runtime java.home",
                javaHome,
                fileProvider.isPresent()
                    ? JavaVersion.toVersion(getResourceContents("/minimumRuntimeVersion"))
                    : JavaVersion.toVersion(bundledJdkMajorVersion)
            )
        );

        Provider<String> vendorDetails = fileProvider.map(j -> metadataDetector.getMetadata(getJavaInstallation(j)))
            .map(m -> formatJavaVendorDetails(m));

        return new RuntimeJava(fileProvider, javaVersion, vendorDetails, explicitlySet, preReleasePostfix, buildNumber);
    }

    private RuntimeJava resolvePreReleaseRuntimeJavaHome(String runtimeJavaProperty, String bundledJdkMajorVersion) {
        var major = JavaLanguageVersion.of(Integer.parseInt(runtimeJavaProperty.substring(0, runtimeJavaProperty.length() - 4)));
        Integer buildNumber = Integer.getInteger("runtime.java.build");
        var jdkbuild = buildNumber == null ? findLatestPreReleaseBuild(major) : findPreReleaseBuild(major, buildNumber);
        String preReleaseType = jdkbuild.type();
        String prVersionString = String.format("%d-%s+%d", major.asInt(), preReleaseType, jdkbuild.buildNumber());
        NamedDomainObjectContainer<Jdk> container = (NamedDomainObjectContainer<Jdk>) project.getExtensions().getByName("jdks");
        Jdk jdk = container.create(preReleaseType + "_" + major.asInt(), j -> {
            j.setVersion(prVersionString);
            j.setVendor("openjdk");
            j.setPlatform(OS.current().javaOsReference);
            j.setArchitecture(Architecture.current().javaClassifier);
            j.setDistributionVersion(preReleaseType);
        });
        // We on purpose resolve this here eagerly to ensure we resolve the jdk configuration in the context of the root project.
        // If we keep this lazy we can not guarantee in which project context this is resolved which will fail the build.
        File file = new File(jdk.getJavaHomePath().toString());
        return runtimeJavaHome(providers.provider(() -> file), true, preReleaseType, jdkbuild.buildNumber(), bundledJdkMajorVersion);
    }

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
