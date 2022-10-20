/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.dra

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest

class DraResolvePluginFuncTest extends AbstractGradleFuncTest {


    def setup() {
        configurationCacheCompatible = false
    }

    def "configures repositories to resolve dra snapshot artifacts"(){
        setup:
        file('buildIds.properties') << """
foo-bar=8.6.0-f633b1d7
        """
        buildFile << """
        plugins {
            id 'elasticsearch.dra-artifacts'
        }
        
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
        
        repositories.all {
            println it.name
        }
        """

        when:
        def build = gradleRunner("resolveDraArtifacts", '-Ddra.artifacts=true', '-g', gradleUserHome, '--refresh-dependencies').buildAndFail()

        then:
        build.output.contains """\
> Could not resolve all files for configuration ':dras'.
   > Could not find org.acme:foo-bar:8.6.0-SNAPSHOT.
     Searched in the following locations:
       - https://artifacts-snapshot.elastic.co/foo-bar/8.6.0-f633b1d7/downloads/foo-bar/foo-bar-8.6.0-SNAPSHOT-deps.zip"""
    }
}
