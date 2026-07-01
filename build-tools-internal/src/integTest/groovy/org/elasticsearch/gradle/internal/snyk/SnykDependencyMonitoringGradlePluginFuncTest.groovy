/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.snyk

import groovy.json.JsonSlurper
import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome
import org.gradle.util.GradleVersion
import spock.lang.Unroll

import static java.net.HttpURLConnection.HTTP_CREATED
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR
import static org.elasticsearch.gradle.fixtures.WiremockFixture.PUT
import static org.elasticsearch.gradle.fixtures.WiremockFixture.withWireMock
import static org.elasticsearch.gradle.internal.snyk.UploadSnykDependenciesGraph.GRADLE_GRAPH_ENDPOINT

class SnykDependencyMonitoringGradlePluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = SnykDependencyMonitoringGradlePlugin.class

    def setup() {
        configurationCacheCompatible = false // configuration is not cc compliant
    }

    @Unroll
    def "can calculate snyk dependency graph"() {
        given:
        buildFile << """
            apply plugin:'java'
            version = "$version"

            repositories {
                mavenCentral()
            }

            dependencies {
                implementation 'org.apache.lucene:lucene-monitor:9.2.0'
            }

            tasks.named('generateSnykDependencyGraph').configure {
                remoteUrl = "http://acme.org"
            }
        """
        when:
        def build = gradleRunner("generateSnykDependencyGraph").build()
        then:
        build.task(":generateSnykDependencyGraph").outcome == TaskOutcome.SUCCESS

        and: "the dependency graph payload describes the resolved Lucene closure"
        // We assert the document structurally rather than via a JSON string comparison: Gradle's
        // ResolvedDependency iteration order is an implementation detail (it changed in 9.6), and
        // every collection in the Snyk payload is conceptually a set. Position-sensitive matching
        // would couple this test to that ordering and break on every Gradle bump.
        def payload = new JsonSlurper().parse(file("build/snyk/dependencies.json"))

        payload.meta == [
            method: "custom gradle",
            id: "gradle",
            node: "v16.15.1",
            name: "gradle",
            plugin: "extern:gradle",
            pluginRuntime: "unknown",
            monitorGraph: true
        ]
        payload.target == [remoteUrl: "http://acme.org", branch: version]
        payload.targetReference == version
        payload.projectAttributes == [lifecycle: [expectedLifecycle]]

        def depGraph = payload.depGraphJSON
        depGraph.schemaVersion == "1.2.0"
        depGraph.pkgManager == [version: GradleVersion.current().version, name: "gradle"]
        depGraph.graph.rootNodeId == "root-node"

        and: "every expected node is present with the right (set of) dependencies"
        def rootPkgId = "hello-world@$version".toString()
        def expectedDepsByNodeId = [
            "root-node"                                      : ["org.apache.lucene:lucene-monitor@9.2.0"] as Set,
            "org.apache.lucene:lucene-monitor@9.2.0"         : [
                "org.apache.lucene:lucene-memory@9.2.0",
                "org.apache.lucene:lucene-analysis-common@9.2.0",
                "org.apache.lucene:lucene-core@9.2.0"
            ] as Set,
            "org.apache.lucene:lucene-memory@9.2.0"          : ["org.apache.lucene:lucene-core@9.2.0"] as Set,
            "org.apache.lucene:lucene-analysis-common@9.2.0" : ["org.apache.lucene:lucene-core@9.2.0"] as Set,
            "org.apache.lucene:lucene-core@9.2.0"            : [] as Set,
        ]
        def expectedPkgIdByNodeId = expectedDepsByNodeId.keySet().collectEntries { id ->
            [(id): id == "root-node" ? rootPkgId : id]
        }

        def actualNodesByNodeId = depGraph.graph.nodes.collectEntries { [(it.nodeId): it] }
        actualNodesByNodeId.keySet() == expectedDepsByNodeId.keySet()
        expectedDepsByNodeId.every { nodeId, expectedDeps ->
            def node = actualNodesByNodeId[nodeId]
            assert node.pkgId == expectedPkgIdByNodeId[nodeId] : "unexpected pkgId for node ${nodeId}"
            def actualDeps = node.deps.collect { it.nodeId } as Set
            assert actualDeps == expectedDeps : "node ${nodeId} deps mismatch: expected ${expectedDeps}, got ${actualDeps}"
            true
        }

        and: "the pkgs section enumerates the same set of packages with matching name/version"
        def actualPkgsById = depGraph.pkgs.collectEntries { [(it.id): it.info] }
        def expectedPkgsById = [
            (rootPkgId)                                      : [name: "hello-world", version: version],
            "org.apache.lucene:lucene-monitor@9.2.0"         : [name: "org.apache.lucene:lucene-monitor", version: "9.2.0"],
            "org.apache.lucene:lucene-memory@9.2.0"          : [name: "org.apache.lucene:lucene-memory", version: "9.2.0"],
            "org.apache.lucene:lucene-core@9.2.0"            : [name: "org.apache.lucene:lucene-core", version: "9.2.0"],
            "org.apache.lucene:lucene-analysis-common@9.2.0" : [name: "org.apache.lucene:lucene-analysis-common", version: "9.2.0"],
        ]
        actualPkgsById == expectedPkgsById

        where:
        version        | expectedLifecycle
        '1.0-SNAPSHOT' | 'development'
        '1.0'          | 'production'
    }

    def "snyk dependency graph deduplicates diamond dependencies and stays reachable"() {
        given:
        buildFile << """
            apply plugin:'java'
            version = "1.0"

            repositories { mavenCentral() }

            dependencies {
                implementation 'org.apache.lucene:lucene-monitor:9.2.0'
                implementation 'org.apache.lucene:lucene-grouping:9.2.0'
                implementation 'org.apache.lucene:lucene-core:9.2.0'
            }

            tasks.named('generateSnykDependencyGraph').configure {
                remoteUrl = "http://acme.org"
            }
        """

        when:
        def result = gradleRunner("generateSnykDependencyGraph").build()
        def json = new JsonSlurper().parse(file("build/snyk/dependencies.json"))
        def graph = json.depGraphJSON.graph
        def nodes = graph.nodes
        def nodesById = nodes.collectEntries { [it.nodeId, it] }

        then:
        result.task(":generateSnykDependencyGraph").outcome == TaskOutcome.SUCCESS

        and: "lucene-core is deduplicated"
        nodes.count { it.nodeId == "org.apache.lucene:lucene-core@9.2.0" } == 1

        and: "lucene-core is referenced from multiple parents"
        def coreParents = nodes.findAll { n ->
            n.deps?.any { it.nodeId == "org.apache.lucene:lucene-core@9.2.0" }
        }*.nodeId
        coreParents.size() >= 2

        and: "every node is reachable from root-node (mirrors Snyk server-side validation)"
        def visited = [] as Set
        def stack = [graph.rootNodeId]
        while (stack) {
            def cur = stack.pop()
            if (visited.add(cur)) {
                def n = nodesById[cur]
                assert n != null : "deps reference unknown node ${cur}"
                n.deps?.each { stack.push(it.nodeId) }
            }
        }
        visited == nodes*.nodeId.toSet()

        and: "every nodeId has a matching pkg entry"
        json.depGraphJSON.pkgs*.id.toSet() == nodes*.pkgId.toSet()
    }

    @Unroll
    def "upload #scenario"() {
        given:
        buildFile << """
            apply plugin:'java'
        """
        when:
        def result = withWireMock(PUT, GRADLE_GRAPH_ENDPOINT, responseBody, httpStatus) { server ->
            buildFile << """
            tasks.named('uploadSnykDependencyGraph').configure {
                getUrl().set('${server.baseUrl()}/api/v1/monitor/gradle/graph')
                getToken().set("myToken")
            }
            """
            if (expectSuccess) {
                gradleRunner("uploadSnykDependencyGraph", '-i', '--stacktrace').build()
            } else {
                gradleRunner("uploadSnykDependencyGraph", '-i').buildAndFail()
            }
        }
        then:
        result.task(":uploadSnykDependencyGraph").outcome == expectedOutcome
        result.output.contains(expectedOutput)

        where:
        scenario                                     | httpStatus          | responseBody     | expectSuccess | expectedOutcome      | expectedOutput
        "succeeds with HTTP 201"                     | HTTP_CREATED        | "OK"             | true          | TaskOutcome.SUCCESS  | "Snyk API call response status: 201"
        "fails with reasonable error message"        | HTTP_INTERNAL_ERROR | "Internal Error" | false         | TaskOutcome.FAILED   | "Uploading Snyk Graph failed with http code 500: Internal Error"
    }
}
