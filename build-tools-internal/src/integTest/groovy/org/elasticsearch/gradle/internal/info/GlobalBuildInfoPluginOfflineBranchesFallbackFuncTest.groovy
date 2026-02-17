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
    def "offline mode falls back to workspace root branches.json for http(s) branches location"() {
        given:
        // This test's build script uses task actions that aren't configuration-cache safe.
        // The behavior under test here is Gradle offline mode fallback, not configuration cache.
        configurationCacheCompatible = false

        propertiesFile << """
            org.elasticsearch.build.branches-file-location=https://example.invalid/branches.json
        """.stripIndent()

        def bwcSource = file("server/src/main/java/org/elasticsearch/Version.java")
        bwcSource.text = """
            package org.elasticsearch;
            public class Version {
                public static final Version V_8_18_2 = new Version(8_18_02_99);
                public static final Version V_9_0_3 = new Version(9_00_03_99);
                public static final Version V_9_1_0 = new Version(9_01_00_99);
                public static final Version CURRENT = V_9_1_0;
            }
        """.stripIndent()

        file("branches.json").text = """
            {
              "branches": [
                { "branch": "main", "version": "9.1.0" },
                { "branch": "9.0", "version": "9.0.3" }
              ]
            }
        """.stripIndent()

        buildFile << """
            plugins {
              id 'elasticsearch.global-build-info'
            }

            tasks.register("resolveBwcVersions") {
              def buildParamsExt = project.extensions.getByName("buildParams")
              doLast {
                println("UNRELEASED_COUNT=" + buildParamsExt.bwcVersions.unreleased.size())
              }
            }
        """.stripIndent()

        when:
        def result = gradleRunner(
            "-DBWC_VERSION_SOURCE=${bwcSource.absolutePath}",
            "--offline",
            "resolveBwcVersions"
        ).build()

        then:
        result.task(":resolveBwcVersions").outcome == TaskOutcome.SUCCESS
        assertOutputContains(result.output, "Gradle is running in offline mode; using local branches.json")
        assertOutputContains(result.output, "UNRELEASED_COUNT=2")
    }
}

