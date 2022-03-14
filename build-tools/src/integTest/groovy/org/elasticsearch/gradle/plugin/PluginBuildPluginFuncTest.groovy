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
            
            tasks.named('explodedBundlePlugin').configure {
                doLast {
                    println outputs.files.files[0].listFiles()
                }
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

}
