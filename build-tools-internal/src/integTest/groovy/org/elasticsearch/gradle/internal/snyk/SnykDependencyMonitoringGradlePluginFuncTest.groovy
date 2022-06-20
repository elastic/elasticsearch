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

        def printGraph(Map entries, int depth = 0 ) {
//            entries.each { k, v ->
//                println k.indent(depth)
//                println v.getClass()
////                printGraph(v, depth++)
//            }
        }
        
        tasks.named('resolveSnykDependencyGraph').configure {
            doLast {
            println graph
                printGraph(graph.nodes)        
            }
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
        def build = gradleRunner("resolveSnykDependencyGraph").build()
        
        then:
        build.task(":resolveSnykDependencyGraph").outcome == TaskOutcome.SUCCESS
    }
}
