/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.transport


import spock.lang.Shared

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.elasticsearch.gradle.fixtures.LocalRepositoryFixture
import org.elasticsearch.gradle.internal.coverage.TransportClassesCoveragePlugin
import org.junit.ClassRule

class TransportClassesCoveragePluginWithNonJavaFuncTest extends AbstractGradleInternalPluginFuncTest {
    Class<? extends TransportClassesCoveragePlugin> pluginClassUnderTest = TransportClassesCoveragePlugin.class

    @Shared
    @ClassRule
    public LocalRepositoryFixture repository = new LocalRepositoryFixture()

    def setup() {
        buildFile << """
            repositories {
                mavenCentral()
            }
            """
        configurationCacheCompatible = false

    }

    def "non java projects will not be scanned"() {
        given:
        when:
        def result = gradleRunner(":transportMethodCoverageVerifier").buildAndFail()

        then:
        result.getOutput().contains("Cannot locate tasks that match ':methodCoverageVerifier' " +
            "as task 'methodCoverageVerifier' not found in root project")
    }

}
