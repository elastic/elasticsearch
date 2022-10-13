/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.jarhell

import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.fixtures.AbstractJavaGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

import static org.elasticsearch.gradle.fixtures.TestClasspathUtils.setupJarHellJar

class JarHellPluginFuncTest extends AbstractJavaGradleFuncTest {

    def setup() {
        buildFile << """
            plugins {
                // bring the build tools on the test classpath
                id 'elasticsearch.esplugin' apply false
            }
            
            repositories {
              maven {
                name = "local-test"
                url = file("local-repo")
                metadataSources {
                  artifact()
                }
              }
            }
        """

        def version = VersionProperties.elasticsearch
        setupJarHellJar(dir("local-repo/org/elasticsearch/elasticsearch-core/${version}/"), version)
    }

    def "skips jar hell task when no java plugin applied"(){
        given:
        buildFile << """
            apply plugin: org.elasticsearch.gradle.jarhell.JarHellPlugin
        """

        when:
        def result = gradleRunner("jarHell").build()
        then:
        result.task(":jarHell").outcome == TaskOutcome.NO_SOURCE
    }

    def "run jar hell when java plugin is applied"(){
        given:
        buildFile << """
            apply plugin: org.elasticsearch.gradle.jarhell.JarHellPlugin
            apply plugin: 'java'
        
        """

        when:
        def result = gradleRunner("jarHell").build()
        then:
        result.task(":jarHell").outcome == TaskOutcome.NO_SOURCE

        when:
        clazz("org.acme.SomeClass")
        result = gradleRunner("jarHell").build()

        then:
        result.task(":jarHell").outcome == TaskOutcome.SUCCESS
    }

    def "jar hell configuration defaults can be overridden"(){
        given:
        def version = "1.0.0"
        clazz("org.acme.SomeClass")

        buildFile << """
            apply plugin: org.elasticsearch.gradle.jarhell.JarHellPlugin
            apply plugin: 'java'
            
            dependencies {
                jarHell "org.elasticsearch:elasticsearch-core:$version"
            }
        """

        setupJarHellJar(dir("local-repo/org/elasticsearch/elasticsearch-core/${version}/"), version)

        when:
        def result = gradleRunner("jarHell").build()
        then:
        result.task(":jarHell").outcome == TaskOutcome.SUCCESS
    }
}
