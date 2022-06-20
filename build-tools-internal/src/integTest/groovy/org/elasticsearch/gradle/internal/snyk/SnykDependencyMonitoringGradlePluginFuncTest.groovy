/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.snyk

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class SnykDependencyMonitoringGradlePluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        buildFile << """
        plugins {
            id 'elasticsearch.snyk-dependency-monitoring'
        }
        """
        configurationCacheCompatible = false // configuration is not cc compliant
    }

    def "can calculate snyk dependency graph"() {
        given:
"".indent(3)
        buildFile << """
            apply plugin:'java'
            
            repositories {
                mavenCentral() 
            }
            
            dependencies {
                implementation 'org.apache.lucene:lucene-monitor:9.2.0'
            }
            
            tasks.named('resolveSnykDependencyGraph').configure {
                configuration = configurations.runtimeClasspath
            }
        """
        when:
        def build = gradleRunner("generateSnykDependencyGraph", "-i", "--stacktrace").build()
        then:
        build.task(":resolveSnykDependencyGraph").outcome == TaskOutcome.SUCCESS

        normalized(file("build/snyk/dependencies.json").text) == """{
    "meta": {
        "method": "custom gradle",
        "id": "gradle",
        "node": "v16.15.1",
        "name": "gradle",
        "plugin": "extern:gradle",
        "pluginRuntime": "unknown",
        "monitorGraph": true,
        "version": "unspecified",
        "versionBuildInfo": {
            "gradleVersion": "7.4.2"
        }
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
                    "pkgId": "hello-world@unspecified"
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
                "id": "hello-world@unspecified",
                "info": {
                    "name": "hello-world",
                    "version": "unspecified"
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
}"""
    }
}
