/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport

import spock.lang.Shared

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.elasticsearch.gradle.fixtures.LocalRepositoryFixture
import org.gradle.testkit.runner.TaskOutcome
import org.junit.ClassRule

class FindTransportClassesNonJavaFuncTest extends AbstractGradleInternalPluginFuncTest {
    Class<? extends FindTransportClassesPlugin> pluginClassUnderTest = FindTransportClassesPlugin.class

    @Shared
    @ClassRule
    public LocalRepositoryFixture repository = new LocalRepositoryFixture()

    def setup() {
        buildFile << """
            apply plugin: 'elasticsearch.build'
            repositories {
                mavenCentral()
            }
            """
    }

    def "non java projects will not fail"() {
        given:
        when:
        def result = gradleRunner(":findTransportClassesTask").build()

        then:
        result.task(":findTransportClassesTask").outcome == TaskOutcome.SUCCESS
    }

}
