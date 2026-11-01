/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import org.gradle.api.InvalidUserDataException

/**
 * Unit tests for the pure-logic helpers in {@link InternalDistributionBwcSetupPlugin}.
 * These tests require no Gradle project, no TestKit, and no network access.
 */
class InternalDistributionBwcSetupPluginSpec extends Specification {

    @TempDir
    File tempDir

    // -------------------------------------------------------------------------
    // validateBwcMode
    // -------------------------------------------------------------------------

    @Unroll
    def "validateBwcMode accepts valid mode '#mode'"() {
        when:
        InternalDistributionBwcSetupPlugin.validateBwcMode(mode)

        then:
        noExceptionThrown()

        where:
        mode << ["gradle", "prebuilt", "dra", "auto"]
    }

    @Unroll
    def "validateBwcMode rejects invalid mode '#mode'"() {
        when:
        InternalDistributionBwcSetupPlugin.validateBwcMode(mode)

        then:
        def ex = thrown(InvalidUserDataException)
        ex.message.contains("Invalid tests.bwc.mode value [${mode}]")
        ex.message.contains("Must be one of: gradle, prebuilt, auto")

        where:
        mode << ["invalid-mode", "DRA", "AUTO", "", "dra-or-gradle"]
    }

    // -------------------------------------------------------------------------
    // buildFallbackMessage — distribution archives
    // -------------------------------------------------------------------------

    def "buildFallbackMessage for prebuilt mode and distribution archive mentions snapshot unavailability"() {
        when:
        def msg = InternalDistributionBwcSetupPlugin.buildFallbackMessage("prebuilt", true, "9.4.2", "darwin-tar")

        then:
        msg.contains("tests.bwc.mode=prebuilt but no pre-built snapshot artifacts were available")
        msg.contains("9.4.2")
        msg.contains("darwin-tar")
        msg.contains("building distribution")
    }

    def "buildFallbackMessage for auto mode and distribution archive mentions commit mismatch"() {
        when:
        def msg = InternalDistributionBwcSetupPlugin.buildFallbackMessage("auto", true, "9.4.2", "linux-tar")

        then:
        msg.contains("pre-built snapshot commit did not match")
        msg.contains("9.4.2")
        msg.contains("linux-tar")
    }

    def "buildFallbackMessage for gradle mode and distribution archive hints at auto mode"() {
        when:
        def msg = InternalDistributionBwcSetupPlugin.buildFallbackMessage("gradle", true, "9.4.2", "windows-zip")

        then:
        msg.contains("-Dtests.bwc.mode=auto")
        msg.contains("9.4.2")
    }

    // -------------------------------------------------------------------------
    // buildFallbackMessage — Maven JAR artifacts (non-archive)
    // -------------------------------------------------------------------------

    def "buildFallbackMessage for prebuilt mode and Maven JAR mentions snapshot unavailability"() {
        when:
        def msg = InternalDistributionBwcSetupPlugin.buildFallbackMessage("prebuilt", false, "9.4.2", "jdbc")

        then:
        msg.contains("tests.bwc.mode=prebuilt but no pre-built snapshot artifacts were available")
        msg.contains("9.4.2")
        msg.contains("jdbc")
        // Maven JAR path does NOT say "distribution" — keep the messages distinct
        !msg.contains("building distribution")
    }

    def "buildFallbackMessage for gradle mode and Maven JAR emits a plain source-build message"() {
        when:
        def msg = InternalDistributionBwcSetupPlugin.buildFallbackMessage("gradle", false, "9.4.2", "logging")

        then:
        msg == "BWC [9.4.2]: building [logging] from source"
    }

    def "buildFallbackMessage for auto mode and Maven JAR emits a plain source-build message"() {
        when:
        def msg = InternalDistributionBwcSetupPlugin.buildFallbackMessage("auto", false, "9.3.5", "plugin-api")

        then:
        msg == "BWC [9.3.5]: building [plugin-api] from source"
    }

    def "seedUniqueGradleUserHome copies gradle config and wrapper distribution into the expected hierarchy"() {
        given:
        File sourceGradleUserHome = new File(tempDir, "source")
        sourceGradleUserHome.mkdirs()
        String wrapperDistributionPath = "wrapper/dists/gradle-9.1-bin"
        new File(sourceGradleUserHome, "gradle.properties").text = "org.gradle.jvmargs=-Xmx1g\n"
        new File(sourceGradleUserHome, "init.d/test.init.gradle").with {
            parentFile.mkdirs()
            text = "println 'seeded'\n"
        }
        writeReadyWrapperDistribution(new File(sourceGradleUserHome, wrapperDistributionPath))
        File uniqueGradleUserHome = new File(tempDir, "unique")

        when:
        InternalDistributionBwcSetupPlugin.seedUniqueGradleUserHome(sourceGradleUserHome, uniqueGradleUserHome, wrapperDistributionPath)

        then:
        new File(uniqueGradleUserHome, "gradle.properties").text == "org.gradle.jvmargs=-Xmx1g\n"
        new File(uniqueGradleUserHome, "init.d/test.init.gradle").text == "println 'seeded'\n"
        InternalDistributionBwcSetupPlugin.isReadyWrapperDistribution(new File(uniqueGradleUserHome, wrapperDistributionPath))
    }

    def "seedUniqueGradleUserHome refreshes config without dropping an existing ready wrapper distribution"() {
        given:
        File sourceGradleUserHome = new File(tempDir, "source")
        sourceGradleUserHome.mkdirs()
        String wrapperDistributionPath = "wrapper/dists/gradle-9.1-bin"
        new File(sourceGradleUserHome, "gradle.properties").text = "org.gradle.jvmargs=-Xmx2g\n"
        new File(sourceGradleUserHome, "init.d/current.init.gradle").with {
            parentFile.mkdirs()
            text = "println 'current'\n"
        }
        File uniqueGradleUserHome = new File(tempDir, "unique")
        new File(uniqueGradleUserHome, "gradle.properties").with {
            parentFile.mkdirs()
            text = "org.gradle.jvmargs=-Xmx512m\n"
        }
        new File(uniqueGradleUserHome, "init.d/stale.init.gradle").with {
            parentFile.mkdirs()
            text = "println 'stale'\n"
        }
        writeReadyWrapperDistribution(new File(uniqueGradleUserHome, wrapperDistributionPath))

        when:
        InternalDistributionBwcSetupPlugin.seedUniqueGradleUserHome(sourceGradleUserHome, uniqueGradleUserHome, wrapperDistributionPath)

        then:
        new File(uniqueGradleUserHome, "gradle.properties").text == "org.gradle.jvmargs=-Xmx2g\n"
        new File(uniqueGradleUserHome, "init.d/current.init.gradle").text == "println 'current'\n"
        new File(uniqueGradleUserHome, "init.d/stale.init.gradle").exists() == false
        InternalDistributionBwcSetupPlugin.isReadyWrapperDistribution(new File(uniqueGradleUserHome, wrapperDistributionPath))
    }

    def "seedUniqueGradleUserHome keeps daemon registry and caches of an existing user home"() {
        given: "an existing unique user home as left behind by a previous nested build and its idle daemon"
        File sourceGradleUserHome = new File(tempDir, "source")
        sourceGradleUserHome.mkdirs()
        String wrapperDistributionPath = "wrapper/dists/gradle-9.1-bin"
        writeReadyWrapperDistribution(new File(sourceGradleUserHome, wrapperDistributionPath))
        File uniqueGradleUserHome = new File(tempDir, "unique")
        File daemonRegistry = new File(uniqueGradleUserHome, "daemon/9.1/registry.bin")
        daemonRegistry.with {
            parentFile.mkdirs()
            text = "registry"
        }
        File cachedJar = new File(uniqueGradleUserHome, "caches/modules-2/files-2.1/foo/foo.jar")
        cachedJar.with {
            parentFile.mkdirs()
            text = "jar"
        }

        when:
        InternalDistributionBwcSetupPlugin.seedUniqueGradleUserHome(sourceGradleUserHome, uniqueGradleUserHome, wrapperDistributionPath)

        then:
        daemonRegistry.text == "registry"
        cachedJar.text == "jar"
        InternalDistributionBwcSetupPlugin.isReadyWrapperDistribution(new File(uniqueGradleUserHome, wrapperDistributionPath))
    }

    def "seedUniqueGradleUserHome replaces an incomplete wrapper distribution"() {
        given:
        File sourceGradleUserHome = new File(tempDir, "source")
        sourceGradleUserHome.mkdirs()
        String wrapperDistributionPath = "wrapper/dists/gradle-9.1-bin"
        writeReadyWrapperDistribution(new File(sourceGradleUserHome, wrapperDistributionPath))
        File uniqueGradleUserHome = new File(tempDir, "unique")
        File partialDownload = new File(uniqueGradleUserHome, wrapperDistributionPath + "/abc123/gradle-9.1-bin.zip.part")
        partialDownload.with {
            parentFile.mkdirs()
            text = ""
        }

        when:
        InternalDistributionBwcSetupPlugin.seedUniqueGradleUserHome(sourceGradleUserHome, uniqueGradleUserHome, wrapperDistributionPath)

        then:
        partialDownload.exists() == false
        InternalDistributionBwcSetupPlugin.isReadyWrapperDistribution(new File(uniqueGradleUserHome, wrapperDistributionPath))
        new File(uniqueGradleUserHome, "wrapper/dists").list().toList() == ["gradle-9.1-bin"]
    }

    def "seedUniqueGradleUserHome syncs init.d in place"() {
        given:
        File sourceGradleUserHome = new File(tempDir, "source")
        String wrapperDistributionPath = "wrapper/dists/gradle-9.1-bin"
        writeReadyWrapperDistribution(new File(sourceGradleUserHome, wrapperDistributionPath))
        new File(sourceGradleUserHome, "init.d/changed.gradle").with {
            parentFile.mkdirs()
            text = "new"
        }
        new File(sourceGradleUserHome, "init.d/added.gradle").text = "added"
        File uniqueGradleUserHome = new File(tempDir, "unique")
        File targetInitDir = new File(uniqueGradleUserHome, "init.d")
        new File(targetInitDir, "changed.gradle").with {
            parentFile.mkdirs()
            text = "old"
        }
        new File(targetInitDir, "stale.gradle").text = "stale"

        when:
        InternalDistributionBwcSetupPlugin.seedUniqueGradleUserHome(sourceGradleUserHome, uniqueGradleUserHome, wrapperDistributionPath)

        then:
        new File(targetInitDir, "changed.gradle").text == "new"
        new File(targetInitDir, "added.gradle").text == "added"
        new File(targetInitDir, "stale.gradle").exists() == false
        targetInitDir.list().toList().toSet() == ["changed.gradle", "added.gradle"].toSet()
    }

    private static void writeReadyWrapperDistribution(File wrapperDistributionDir) {
        String extractedGradleDirName = wrapperDistributionDir.name.endsWith("-bin")
            ? wrapperDistributionDir.name.substring(0, wrapperDistributionDir.name.length() - "-bin".length())
            : wrapperDistributionDir.name
        File hashDir = new File(wrapperDistributionDir, "abc123")
        new File(hashDir, wrapperDistributionDir.name + ".zip.ok").with {
            parentFile.mkdirs()
            text = ""
        }
        new File(hashDir, extractedGradleDirName + "/bin/gradle").with {
            parentFile.mkdirs()
            text = "#!/bin/sh\n"
        }
    }
}
