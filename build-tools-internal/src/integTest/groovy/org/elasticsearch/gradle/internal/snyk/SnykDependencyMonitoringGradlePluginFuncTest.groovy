/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.snyk

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome
import org.gradle.util.GradleVersion
import org.skyscreamer.jsonassert.JSONAssert

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
        """
        when:
        def build = gradleRunner("generateSnykDependencyGraph").build()
        then:
        build.task(":generateSnykDependencyGraph").outcome == TaskOutcome.SUCCESS
        JSONAssert.assertEquals(file("build/snyk/dependencies.json").text, """{
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
                "remoteUrl": "http://github.com/elastic/elasticsearch.git",
                "branch": "unknown"
            },
            "targetReference": "$version",
            "projectAttributes": {
                "lifecycle": [
                  "$expectedLifecycle"
                ]
            }
        }""", true)

        where:
        version        | expectedLifecycle
        '1.0-SNAPSHOT' | 'development'
        '1.0'          | 'production'
    }

    def "upload fails with reasonable error message"() {
        given:
        buildFile << """
            apply plugin:'java'
        """
        when:
        def result = withWireMock(PUT, "/api/v1/monitor/gradle/graph", "OK", HTTP_CREATED) { server ->
            buildFile << """
            tasks.named('uploadSnykDependencyGraph').configure {
                getUrl().set('${server.baseUrl()}/api/v1/monitor/gradle/graph')
                getToken().set("myToken")
            }
            """
            gradleRunner("uploadSnykDependencyGraph", '-i', '--stacktrace').build()
        }
        then:
        result.task(":uploadSnykDependencyGraph").outcome == TaskOutcome.SUCCESS
        result.output.contains("Snyk API call response status: 201")

        when:
        result = withWireMock(PUT, GRADLE_GRAPH_ENDPOINT, "Internal Error", HTTP_INTERNAL_ERROR) { server ->
            buildFile << """
            tasks.named('uploadSnykDependencyGraph').configure {
                getUrl().set('${server.baseUrl()}/api/v1/monitor/gradle/graph')
            }
            """
            gradleRunner("uploadSnykDependencyGraph", '-i').buildAndFail()
        }

        then:
        result.task(":uploadSnykDependencyGraph").outcome == TaskOutcome.FAILED
        result.output.contains("Uploading Snyk Graph failed with http code 500: Internal Error")
    }
}
