/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.fixtures

import org.apache.commons.io.FileUtils
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner

import java.nio.file.Files
import java.nio.file.Path

/**
 * Test fixture for mocking JDK toolchain provisioning in Gradle tests.
 *
 * Since Gradle 9.x enforces HTTPS for toolchain downloads and there's no way to bypass this,
 * we instead pre-install a fake JDK in a temp directory and configure Gradle to detect it
 * via the org.gradle.java.installations.paths property. This avoids the need for any network
 * operations during the test.
 */
class JdkToolchainTestFixture {

    /**
     * Runs a Gradle build with a pre-installed fake JDK that Gradle will detect as a valid toolchain.
     *
     * @param gradleRunner The GradleRunner to configure
     * @param buildRunClosure Closure that executes the build
     * @param javaVersion The Java version to simulate (e.g., 17)
     * @param vendor The vendor name (e.g., "eclipse_adoptium")
     * @return The BuildResult from running the build
     */
    static BuildResult withMockedJdkDownload(GradleRunner gradleRunner,
                                             Closure<BuildResult> buildRunClosure,
                                             int javaVersion,
                                             String vendor) {
        // Create a temp directory to hold the fake JDK installation
        Path tempDir = Files.createTempDirectory("fake-jdk-")
        try {
            // Create a fake JDK installation structure that Gradle can detect
            Path jdkInstallDir = createFakeJdkInstallation(tempDir, javaVersion, vendor)

            // Configure Gradle to find our fake JDK via installations.paths
            // Disable auto-detect completely to force Gradle to use only our fake JDK
            List<String> givenArguments = gradleRunner.getArguments()

            // Remove any auto-detect or installations.paths settings - we'll set our own
            List<String> filteredArguments = givenArguments.findAll { arg ->
                String argStr = arg.toString()
                !argStr.contains('org.gradle.java.installations.auto-detect') &&
                    !argStr.contains('org.gradle.java.installations.paths')
            }

            // Use toRealPath() to get the canonical path (with /private prefix on macOS)
            // This ensures consistency between the path we pass and the path Gradle resolves
            List<String> addedArguments = [
                "-Dorg.gradle.java.installations.paths=${jdkInstallDir.toRealPath()}".toString(),
                "-Dorg.gradle.java.installations.auto-detect=false",
                "-Dorg.gradle.java.installations.auto-download=false"
            ]

            List<String> effectiveArguments = (filteredArguments + addedArguments).collect { it.toString() }

            GradleRunner effectiveRunner = gradleRunner.withArguments(effectiveArguments)
            buildRunClosure.delegate = effectiveRunner
            return buildRunClosure.call(effectiveRunner)
        } finally {
            // Clean up temp directory
            FileUtils.deleteDirectory(tempDir.toFile())
        }
    }

    static BuildResult withMockedJdkDownload(GradleRunner gradleRunner, int javaVersion, String vendor) {
        return withMockedJdkDownload(gradleRunner, { it.build() }, javaVersion, vendor)
    }

    /**
     * Creates a fake JDK installation structure that Gradle's toolchain detection will recognize.
     *
     * The directory structure follows the convention that Gradle expects:
     * - For macOS: jdk-<version>/Contents/Home/bin/java and jdk-<version>/Contents/Home/release
     * - For Linux/Windows: jdk-<version>/bin/java and jdk-<version>/release
     *
     * The directory is named to include the vendor so that the path contains "eclipse_adoptium-17"
     * which the test expects to see in ES_JAVA_HOME output.
     */
    private static Path createFakeJdkInstallation(Path baseDir, int javaVersion, String vendor) {
        // Create a directory name that includes vendor-version for test assertions
        // e.g., "eclipse_adoptium-17.0.0"
        String dirName = "${vendor}-${javaVersion}.0.0"
        Path jdkDir = baseDir.resolve(dirName)
        Files.createDirectories(jdkDir)

        // Determine if we're on macOS (different JDK structure)
        boolean isMacOS = System.getProperty("os.name").toLowerCase().contains("mac")

        Path homeDir
        if (isMacOS) {
            // macOS structure: jdk/Contents/Home/
            homeDir = jdkDir.resolve("Contents").resolve("Home")
        } else {
            // Linux/Windows structure: jdk/
            homeDir = jdkDir
        }
        Files.createDirectories(homeDir)

        // Create bin directory with java executable
        Path binDir = homeDir.resolve("bin")
        Files.createDirectories(binDir)

        Path javaExecutable = binDir.resolve("java")
        // Create a shell script that handles Gradle's JVM probing
        // Gradle runs: java -Xmx32m -Xms32m -cp . JavaProbe
        // Gradle's MetadataProbe generates a JavaProbe.class that outputs lines in format:
        //   GRADLE_PROBE_VALUE:<property_value>
        // The order of values matches ProbedSystemProperty enum:
        //   java.home, java.version, java.vendor, java.runtime.name, java.runtime.version,
        //   java.vm.name, java.vm.version, java.vm.vendor, os.arch
        String vendorName = vendorToDisplayName(vendor)
        String osArch = System.getProperty("os.arch")
        // Use toRealPath() to get the canonical path with /private prefix on macOS
        // This is critical because Gradle resolves paths to their canonical form
        // and expects JAVA_HOME output to match exactly
        String homePath = homeDir.toRealPath().toString()

        // Gradle's JavaProbe outputs GRADLE_PROBE_VALUE:<value> lines, one per property in enum order
        String script = """\
#!/bin/bash
# Fake Java ${javaVersion} executable for Gradle toolchain testing

# Gradle runs: java -cp . JavaProbe
# We detect this by checking if the last argument is "JavaProbe"
LAST_ARG="\${@: -1}"

if [ "\$LAST_ARG" = "JavaProbe" ]; then
    # Output JVM metadata in the format Gradle expects (GRADLE_PROBE_VALUE:<value>)
    # Order MUST match ProbedSystemProperty enum:
    # java.home, java.version, java.vendor, java.runtime.name, java.runtime.version,
    # java.vm.name, java.vm.version, java.vm.vendor, os.arch
    echo "GRADLE_PROBE_VALUE:${homePath}"
    echo "GRADLE_PROBE_VALUE:${javaVersion}.0.0"
    echo "GRADLE_PROBE_VALUE:${vendorName}"
    echo "GRADLE_PROBE_VALUE:OpenJDK Runtime Environment"
    echo "GRADLE_PROBE_VALUE:${javaVersion}.0.0+0"
    echo "GRADLE_PROBE_VALUE:OpenJDK 64-Bit Server VM"
    echo "GRADLE_PROBE_VALUE:${javaVersion}.0.0+0"
    echo "GRADLE_PROBE_VALUE:${vendorName}"
    echo "GRADLE_PROBE_VALUE:${osArch}"
    exit 0
fi

# Handle standard -version calls
if [ "\$1" = "-version" ] || [ "\$1" = "--version" ]; then
    echo 'openjdk version "${javaVersion}.0.0" 2024-01-01' >&2
    echo 'OpenJDK Runtime Environment Temurin-${javaVersion}.0.0+0 (build ${javaVersion}.0.0+0)' >&2
    echo 'OpenJDK 64-Bit Server VM Temurin-${javaVersion}.0.0+0 (build ${javaVersion}.0.0+0, mixed mode)' >&2
    exit 0
fi

# For any other invocation, just exit successfully
exit 0
"""
        Files.write(javaExecutable, script.getBytes())
        // Make it executable
        javaExecutable.toFile().setExecutable(true, false)

        // Create release file with proper metadata that Gradle uses to identify the JDK
        Path releaseFile = homeDir.resolve("release")
        String releaseContent = """\
IMPLEMENTOR="${vendorName}"
IMPLEMENTOR_VERSION="Temurin-${javaVersion}.0.0+0"
JAVA_RUNTIME_VERSION="${javaVersion}.0.0+0"
JAVA_VERSION="${javaVersion}.0.0"
JAVA_VERSION_DATE="2024-01-01"
OS_ARCH="${osArch}"
OS_NAME="${isMacOS ? 'Darwin' : 'Linux'}"
FULL_VERSION="${javaVersion}.0.0+0"
BUILD_INFO="Fake JDK for testing"
JVM_VARIANT="Hotspot"
JVM_VERSION="${javaVersion}.0.0+0"
IMAGE_TYPE="JDK"
"""
        Files.write(releaseFile, releaseContent.getBytes())

        // Return the JDK home path (where bin/java and release live) for org.gradle.java.installations.paths
        return homeDir
    }

    /**
     * Converts vendor identifier to display name used in release file.
     */
    private static String vendorToDisplayName(String vendor) {
        switch (vendor) {
            case "eclipse_adoptium":
                return "Eclipse Adoptium"
            case "adoptopenjdk":
                return "AdoptOpenJDK"
            case "azul":
                return "Azul Systems, Inc."
            case "amazon":
                return "Amazon.com Inc."
            case "bellsoft":
                return "BellSoft"
            case "oracle":
                return "Oracle Corporation"
            default:
                return vendor
        }
    }

}
