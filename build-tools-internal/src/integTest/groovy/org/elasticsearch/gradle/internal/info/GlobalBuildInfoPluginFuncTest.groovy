/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.info

import com.sun.net.httpserver.HttpServer

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

class GlobalBuildInfoPluginFuncTest extends AbstractGradleFuncTest {
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

    def "retries branches.json download from flaky http endpoint"() {
        given:
        configurationCacheCompatible = false
        writeBwcVersionSource()
        writeBuildThatResolvesBwcVersions()

        AtomicInteger requests = new AtomicInteger()
        byte[] body = branchesJsonBody().getBytes(StandardCharsets.UTF_8)
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)
        server.createContext(
            "/branches.json", { exchange ->
            int request = requests.getAndIncrement()
            if (request == 0) {
                exchange.sendResponseHeaders(500, 0)
                exchange.responseBody.close()
            } else {
                exchange.sendResponseHeaders(200, body.length)
                exchange.responseBody.withCloseable { os -> os.write(body) }
            }
            exchange.close()
        } as com.sun.net.httpserver.HttpHandler
        )
        server.start()

        and:
        String url = branchesUrl(server)

        when:
        def result = gradleRunner(
            "resolveBwcVersions",
            "-Porg.elasticsearch.build.branches-file-location=${url}",
            "-DBWC_VERSION_SOURCE=${file("Version.java").absolutePath}"
        ).build()

        then:
        result.task(":resolveBwcVersions").outcome == TaskOutcome.SUCCESS
        requests.get() == 2

        cleanup:
        server.stop(0)
    }

    def "exhausts retries when branches.json http endpoint keeps failing"() {
        given:
        configurationCacheCompatible = false
        writeBwcVersionSource()
        writeBuildThatResolvesBwcVersions()

        AtomicInteger requests = new AtomicInteger()
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)
        server.createContext(
            "/branches.json", { exchange ->
            requests.incrementAndGet()
            exchange.sendResponseHeaders(500, 0)
            exchange.responseBody.close()
            exchange.close()
        } as com.sun.net.httpserver.HttpHandler
        )
        server.start()

        and:
        String url = branchesUrl(server)

        when:
        def result = gradleRunner(
            "resolveBwcVersions",
            "-Porg.elasticsearch.build.branches-file-location=${url}",
            "-DBWC_VERSION_SOURCE=${file("Version.java").absolutePath}"
        ).buildAndFail()

        then:
        assertOutputContains(result.output, "Failed to download branches.json from:")
        requests.get() == 3

        cleanup:
        server.stop(0)
    }

    private static String branchesUrl(HttpServer server) {
        String host = server.address.address.hostAddress
        return new URI("http", null, host, server.address.port, "/branches.json", null, null).toString()
    }

    private void writeBuildThatResolvesBwcVersions() {
        buildFile.text = """
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
    }

    private String branchesJsonBody() {
        return """\
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

