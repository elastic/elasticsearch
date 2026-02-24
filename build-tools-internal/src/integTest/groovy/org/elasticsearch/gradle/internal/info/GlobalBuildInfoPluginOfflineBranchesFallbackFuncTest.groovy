/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.info

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class GlobalBuildInfoPluginOfflineBranchesFallbackFuncTest extends AbstractGradleFuncTest {

    def setup() {
        // This plugin reads files from the workspace and performs environment detection
        // that is not compatible with the configuration cache in functional tests.
        configurationCacheCompatible = false
    }

    def "offline mode falls back to local branches.json when present"() {
        given:
        writeBwcVersionSource()
        writeBranchesJson()
        writeBuildThatResolvesBwcVersions()

        when:
        def result = gradleRunner(
            "resolveBwcVersions",
            "--offline",
            // Make any accidental outbound HTTPS attempt fail deterministically.
            "-Dhttps.proxyHost=127.0.0.1",
            "-Dhttps.proxyPort=1",
            "-Dhttp.proxyHost=127.0.0.1",
            "-Dhttp.proxyPort=1",
            "-DBWC_VERSION_SOURCE=${file("Version.java").absolutePath}"
        ).build()

        then:
        result.task(":resolveBwcVersions").outcome == TaskOutcome.SUCCESS
        assertOutputContains(
            result.output,
            "Gradle is running in offline mode; falling back to local branches.json"
        )
        assertOutputContains(result.output, "BWC_VERSIONS_RESOLVED")
    }

    def "offline mode without local branches.json retains failure behavior"() {
        given:
        writeBwcVersionSource()
        // Intentionally do not create branches.json
        writeBuildThatResolvesBwcVersions()

        when:
        def result = gradleRunner(
            "resolveBwcVersions",
            "--offline",
            "-Dhttps.proxyHost=127.0.0.1",
            "-Dhttps.proxyPort=1",
            "-Dhttp.proxyHost=127.0.0.1",
            "-Dhttp.proxyPort=1",
            "-DBWC_VERSION_SOURCE=${file("Version.java").absolutePath}"
        ).buildAndFail()

        then:
        assertOutputContains(result.output, "Failed to download branches.json from:")
        assertOutputMissing(result.output, "falling back to local branches.json")
    }

    def "non-offline mode does not fall back to local branches.json"() {
        given:
        writeBwcVersionSource()
        writeBranchesJson()
        writeBuildThatResolvesBwcVersions()

        when:
        def result = gradleRunner(
            "resolveBwcVersions",
            // Not passing --offline on purpose.
            "-Dhttps.proxyHost=127.0.0.1",
            "-Dhttps.proxyPort=1",
            "-Dhttp.proxyHost=127.0.0.1",
            "-Dhttp.proxyPort=1",
            "-DBWC_VERSION_SOURCE=${file("Version.java").absolutePath}"
        ).buildAndFail()

        then:
        assertOutputContains(result.output, "Failed to download branches.json from:")
        assertOutputMissing(result.output, "falling back to local branches.json")
    }

    private void writeBuildThatResolvesBwcVersions() {
        buildFile.text = """
            plugins {
              id 'elasticsearch.global-build-info'
            }

            tasks.register("resolveBwcVersions") {
              doLast {
                // Force bwcVersions resolution, which triggers branches.json loading.
                def bwcVersions = buildParams.bwcVersionsProvider.get()
                println "BWC_VERSIONS_RESOLVED=[" + bwcVersions + "]"
              }
            }
        """
    }

    private void writeBranchesJson() {
        file("branches.json").text = """\
            {
              "branches": [
                { "branch": "main", "version": "9.1.0" },
                { "branch": "9.0", "version": "9.0.0" }
              ]
            }
        """.stripIndent()
    }

    private void writeBwcVersionSource() {
        // This file is parsed via regex; version constants must have a non-word
        // character before `public` and must end with `);` to match the pattern.
        file("Version.java").text = """\
package org.elasticsearch;

public class Version {
  // Only used by GlobalBuildInfoPlugin in tests via regex parsing.
   public static final Version V_9_0_0 = Version.fromId(9000000);
   public static final Version V_9_1_0 = Version.fromId(9010000);

  private static Version fromId(int id) {
    return new Version();
  }
}
"""
    }
}

