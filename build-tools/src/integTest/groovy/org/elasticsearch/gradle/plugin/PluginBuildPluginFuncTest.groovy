/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class PluginBuildPluginFuncTest extends AbstractGradleFuncTest {

    def "can assemble plugin via #taskName"() {
        given:
        buildFile << """plugins {
                id 'elasticsearch.esplugin'
            }
            
            esplugin { 
                description = 'test plugin'
                classname = 'com.acme.plugin.TestPlugin'
            }
            
            // for testing purposes only
            configurations.compileOnly.dependencies.clear()
            """

        when:
        def result = gradleRunner(taskName).build()

        then:
        result.task(taskName).outcome == TaskOutcome.SUCCESS
        file(expectedOutputPath).exists()

        where:
        expectedOutputPath                    | taskName
        "build/distributions/hello-world.zip" | ":bundlePlugin"
        "build/explodedBundle/"               | ":explodedBundlePlugin"

    }

    def "can resolve plugin as directory without intermediate zipping "() {
        given:
        buildFile << """plugins {
                id 'elasticsearch.esplugin'
            }
            
            esplugin { 
                name = 'sample-plugin'
                description = 'test plugin'
                classname = 'com.acme.plugin.TestPlugin'
            }
            
            // for testing purposes only
            configurations.compileOnly.dependencies.clear()
            """

        file('settings.gradle') << "include 'module-consumer'"
        file('module-consumer/build.gradle') << """
            configurations {
                consume
            }
            
            dependencies {
                consume project(path:':', configuration:'${PluginBuildPlugin.EXPLODED_BUNDLE_CONFIG}')
            }
            
            tasks.register("resolveModule", Copy) {
                from configurations.consume
                into "build/resolved"
            }
        """
        when:
        def result = gradleRunner(":module-consumer:resolveModule").build()

        then:
        result.task(":module-consumer:resolveModule").outcome == TaskOutcome.SUCCESS
        result.task(":explodedBundlePlugin").outcome == TaskOutcome.SUCCESS
        file("module-consumer/build/resolved/sample-plugin.jar").exists()
        file("module-consumer/build/resolved/plugin-descriptor.properties").exists()
    }
}
