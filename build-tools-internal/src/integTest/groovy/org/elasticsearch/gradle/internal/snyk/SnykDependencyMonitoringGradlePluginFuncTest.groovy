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
import org.skyscreamer.jsonassert.JSONAssert
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
        JSONAssert.assertEquals("""{
            "meta": {
                "method": "custom gradle",
                "id": "gradle",
                "node": "v16.15.1",
                "name": "gradle",
                "plugin": "extern:gradle",
                "pluginRuntime": "unknown",
                "monitorGraph": true
            },
            "depGraphJSON": {
                "pkgManager": {
                    "version": "${GradleVersion.current().version}",
                    "name": "gradle"
                },
                "schemaVersion": "1.2.0",
                "graph": {
                    "rootNodeId": "root-node",
                    "nodes": [
                        {
                            "nodeId": "root-node",
                            "deps": [
                                {
                                    "nodeId": "org.apache.lucene:lucene-monitor@9.2.0"
                                }
                            ],
                            "pkgId": "hello-world@$version"
                        },
                        {
                            "nodeId": "org.apache.lucene:lucene-monitor@9.2.0",
                            "deps": [
                                {
                                    "nodeId": "org.apache.lucene:lucene-memory@9.2.0"
                                },
                                {
                                    "nodeId": "org.apache.lucene:lucene-analysis-common@9.2.0"
                                },
                                {
                                    "nodeId": "org.apache.lucene:lucene-core@9.2.0"
                                }
                            ],
                            "pkgId": "org.apache.lucene:lucene-monitor@9.2.0"
                        },
                        {
                            "nodeId": "org.apache.lucene:lucene-memory@9.2.0",
                            "deps": [
                                {
                                    "nodeId": "org.apache.lucene:lucene-core@9.2.0"
                                }
                            ],
                            "pkgId": "org.apache.lucene:lucene-memory@9.2.0"
                        },
                        {
                            "nodeId": "org.apache.lucene:lucene-core@9.2.0",
                            "deps": [

                            ],
                            "pkgId": "org.apache.lucene:lucene-core@9.2.0"
                        },
                        {
                            "nodeId": "org.apache.lucene:lucene-analysis-common@9.2.0",
                            "deps": [
                                {
                                    "nodeId": "org.apache.lucene:lucene-core@9.2.0"
                                }
                            ],
                            "pkgId": "org.apache.lucene:lucene-analysis-common@9.2.0"
                        }
                    ]
                },
                "pkgs": [
                    {
                        "id": "hello-world@$version",
                        "info": {
                            "name": "hello-world",
                            "version": "$version"
                        }
                    },
                    {
                        "id": "org.apache.lucene:lucene-monitor@9.2.0",
                        "info": {
                            "name": "org.apache.lucene:lucene-monitor",
                            "version": "9.2.0"
                        }
                    },
                    {
                        "id": "org.apache.lucene:lucene-memory@9.2.0",
                        "info": {
                            "name": "org.apache.lucene:lucene-memory",
                            "version": "9.2.0"
                        }
                    },
                    {
                        "id": "org.apache.lucene:lucene-core@9.2.0",
                        "info": {
                            "name": "org.apache.lucene:lucene-core",
                            "version": "9.2.0"
                        }
                    },
                    {
                        "id": "org.apache.lucene:lucene-analysis-common@9.2.0",
                        "info": {
                            "name": "org.apache.lucene:lucene-analysis-common",
                            "version": "9.2.0"
                        }
                    }
                ]
            },
            "target": {
                "remoteUrl": "http://acme.org",
                "branch": "$version"
            },
            "targetReference": "$version",
            "projectAttributes": {
                "lifecycle": [
                  "$expectedLifecycle"
                ]
            }
        }""", file("build/snyk/dependencies.json").text, true)

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
