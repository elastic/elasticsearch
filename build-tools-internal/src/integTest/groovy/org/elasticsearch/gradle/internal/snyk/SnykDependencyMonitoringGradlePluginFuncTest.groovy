/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.snyk

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.elasticsearch.gradle.fixtures.http.HttpServerRule
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome
import org.junit.Rule
import org.skyscreamer.jsonassert.JSONAssert

class SnykDependencyMonitoringGradlePluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    @Rule
    public HttpServerRule httpServer = new HttpServerRule()

    Class<? extends Plugin> pluginClassUnderTest = SnykDependencyMonitoringGradlePlugin.class

    def setup() {
        configurationCacheCompatible = false // configuration is not cc compliant
    }

    def "can calculate snyk dependency graph"() {
        given:
        buildFile << """
            apply plugin:'java'
            version = "1.0-SNAPSHOT"

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
        JSONAssert.assertEquals(file( "build/snyk/dependencies.json").text, """{
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
                    "version": "7.4.2",
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
                            "pkgId": "hello-world@1.0-SNAPSHOT"
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
                        "id": "hello-world@1.0-SNAPSHOT",
                        "info": {
                            "name": "hello-world",
                            "version": "1.0-SNAPSHOT"
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
            }
        }""", true)
    }

    def "upload fails with reasonable error message"() {
        given:
        httpServer.registerHandler("/api/v1/monitor/gradle/graph") { handler ->
            handler.responseBody = "Success!"
            handler.expectedHttpResponseCode = HttpURLConnection.HTTP_CREATED
        }

        buildFile << """
            apply plugin:'java'
            
            tasks.named('uploadSnykDependencyGraph').configure {
                getUrl().set("${httpServer.getUriFor('/api/v1/monitor/gradle/graph')}")
                getToken().set("myToken")
            }
        """
        when:
        def result = gradleRunner("uploadSnykDependencyGraph", '-i').build()
        then:
        result.task(":uploadSnykDependencyGraph").outcome == TaskOutcome.SUCCESS
        result.output.contains("Snyk API call response status: 201")

        when:
        httpServer.registerHandler("/api/v1/monitor/gradle/graph") { handler ->
            handler.responseBody = "Internal Error"
            handler.expectedHttpResponseCode = HttpURLConnection.HTTP_INTERNAL_ERROR
        }

        result = gradleRunner("uploadSnykDependencyGraph").buildAndFail()

        then:
        result.task(":uploadSnykDependencyGraph").outcome == TaskOutcome.FAILED
        result.output.contains("Uploading Snyk Graph failed with http code 500: Internal Error")
    }
}
