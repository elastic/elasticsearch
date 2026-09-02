/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.testkit.runner.TaskOutcome

class ElasticsearchDistributionPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends org.gradle.api.Plugin> pluginClassUnderTest = org.elasticsearch.gradle.internal.distribution.ElasticsearchDistributionPlugin

    
    def "copied modules are resolved from explodedBundleZip"() {
        given:
        disableConfigurationCache("esplugin plugin is not configuration cache compatible yet")
        moduleSubProject()

        // elasticsearch.distro is applied by AbstractGradleInternalPluginFuncTest
        buildFile << """
            def someCopy = tasks.register('someCopy', Sync) {
                into 'build/targetDir'
            }

            distro.copyModule(someCopy, project(":module"))
            """
        when:
        def result = gradleRunner("someCopy").build()

        then:
        result.task(":someCopy").outcome == TaskOutcome.SUCCESS
        file('build/targetDir/modules/some-test-module/some-test-module.jar').exists()
        file('build/targetDir/modules/some-test-module/plugin-descriptor.properties').exists()
    }

    private File moduleSubProject() {
        settingsFile << "include 'module'"
        file('module/build.gradle') << """
            plugins {
                id 'elasticsearch.esplugin'
            }

            esplugin {
                name = 'some-test-module'
                classname = 'org.acme.never.used.TestPluginClass'
                description = 'some plugin description'
            }

            // for testing purposes only
            configurations.compileOnly.dependencies.clear()
        """
    }
}
