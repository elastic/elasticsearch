/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

class InternalResourceExhaustionTestPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = InternalResourceExhaustionTestPlugin.class

    def "plugin applies without error and registers resourceExhaustionTest task"() {
        given:
        buildFile << """
            afterEvaluate {
                def task = tasks.findByName('resourceExhaustionTest')
                assert task != null : "resourceExhaustionTest task must be registered"
                assert task.forkEvery == 1L : "forkEvery must be 1 for JVM isolation"
            }
        """

        when:
        def result = gradleRunner("help").build()

        then:
        result.task(":help").outcome == TaskOutcome.SUCCESS
    }
}
