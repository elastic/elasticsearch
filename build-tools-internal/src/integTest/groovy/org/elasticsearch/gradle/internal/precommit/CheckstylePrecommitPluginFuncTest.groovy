/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.elasticsearch.gradle.fixtures.LocalRepositoryFixture
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin
import org.gradle.testkit.runner.TaskOutcome
import org.junit.ClassRule
import spock.lang.Shared

/**
 * This just tests basic plugin configuration, wiring and compatibility and not checkstyle itself
 * */
class CheckstylePrecommitPluginFuncTest extends AbstractGradleInternalPluginFuncTest {
    Class<? extends PrecommitPlugin> pluginClassUnderTest = CheckstylePrecommitPlugin.class

    @Shared
    @ClassRule
    public LocalRepositoryFixture repository = new LocalRepositoryFixture()

    def setup() {
        withVersionCatalogue()
        buildFile << """
        apply plugin:'java'
        """
        repository.configureBuild(buildFile)
        repository.generateJar("org.elasticsearch", "build-conventions", "unspecified", 'org.acme.CheckstyleStuff')
        repository.generateJar("com.puppycrawl.tools", "checkstyle", "10.3", 'org.puppycral.CheckstyleStuff')

    }

    def "can configure checkstyle tasks"() {
        when:
        def result = gradleRunner("precommit").build()
        then:
        result.task(":checkstyleMain").outcome == TaskOutcome.NO_SOURCE
        result.task(":checkstyleTest").outcome == TaskOutcome.NO_SOURCE
    }
}
