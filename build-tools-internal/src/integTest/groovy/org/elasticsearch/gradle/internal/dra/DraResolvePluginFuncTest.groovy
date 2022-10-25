/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.dra

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.elasticsearch.gradle.fixtures.WiremockFixture
import org.gradle.testkit.runner.TaskOutcome

class DraResolvePluginFuncTest extends AbstractGradleFuncTest {


    def setup() {
        configurationCacheCompatible = false

        buildFile << """
        plugins {
            id 'elasticsearch.dra-artifacts'
        }
        
        repositories.all {
            // for supporting http testing repos here
            allowInsecureProtocol = true
        }
        """
    }

    def "configures repositories to resolve dra snapshot artifacts"(){
        setup:
        file('buildIds.properties') << """
foo-bar=8.6.0-f633b1d7
        """
        buildFile << """
        configurations {
            dras
        }
        
        dependencies {
            dras "org.acme:foo-bar:8.6.0-SNAPSHOT:deps@zip"
        }
        
        tasks.register('resolveDraArtifacts') {
            doLast {
                configurations.dras.files.each { println it }
            }
        }
        """

        when:
        def result = WiremockFixture.withWireMock("/foo-bar/8.6.0-f633b1d7/downloads/foo-bar/foo-bar-8.6.0-SNAPSHOT-deps.zip",
                "content".getBytes('UTF-8')) { server ->
            gradleRunner("resolveDraArtifacts",
                    '-Ddra.artifacts=true',
                    "-Ddra.artifacts.url.prefix=${server.baseUrl()}",
                    '-g', gradleUserHome, '--refresh-dependencies').build()
        }

        then:
        result.task(":resolveDraArtifacts").outcome == TaskOutcome.SUCCESS
    }
}
