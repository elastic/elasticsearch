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

/**
 * Unit tests for the pure-logic helpers in {@link InternalDistributionBwcSetupPlugin}.
 * These tests require no Gradle project, no TestKit, and no network access.
 */
class InternalDistributionBwcSetupPluginSpec extends Specification {

    @TempDir
    File tempDir

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
