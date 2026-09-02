/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin
import org.gradle.testkit.runner.TaskOutcome

class ForbiddenApisPrecommitPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends PrecommitPlugin> pluginClassUnderTest = ForbiddenApisPrecommitPlugin.class

    def setup() {
        buildFile << """
        apply plugin:'java'
        """
    }

    def "configures forbidden apis tasks for main and test source sets"() {
        when:
        def result = gradleRunner("tasks", "--all").build()

        then:
        result.output.contains("forbiddenApisMain")
        result.output.contains("forbiddenApisTest")
        result.output.contains("forbiddenApisResources")
    }

    def "invokes CheckForbiddenApisTask with simplified signatures"() {
        given:
        buildFile << """
        import org.elasticsearch.gradle.internal.precommit.CheckForbiddenApisTask

        tasks.named('forbiddenApisMain', CheckForbiddenApisTask).configure {
            bundledSignatures.set([] as Set)
            replaceSignatureFiles('jdk-signatures')
        }
        tasks.named('forbiddenApisTest', CheckForbiddenApisTask).configure {
            bundledSignatures.set([] as Set)
            replaceSignatureFiles('jdk-signatures')
        }
        """
        clazz('org.acme.Main')
        testClazz('org.acme.MainTests')

        when:
        def result = gradleRunner('forbiddenApis').build()

        then:
        result.task(':forbiddenApisResources').outcome == TaskOutcome.SUCCESS
        result.task(':forbiddenApisMain').outcome == TaskOutcome.SUCCESS
        result.task(':forbiddenApisTest').outcome == TaskOutcome.SUCCESS
        result.task(':forbiddenApis').outcome == TaskOutcome.SUCCESS
    }
}
