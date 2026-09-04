/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin

/**
 * Smoke test that applies the plugin in a real Gradle invocation. Detailed wiring assertions
 * live in {@code JmhPluginSpec} (unit-level via {@code ProjectBuilder}); this test only
 * guards against configuration-time errors that unit tests cannot surface.
 */
class JmhPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = JmhPlugin

    def "plugin can be applied and Gradle can configure the project"() {
        // Running `tasks --group benchmark` forces plugin apply and full configuration;
        // the benchmark runner task is registered in that group so it must be listed.
        when:
        def result = gradleRunner('tasks', '--group', 'benchmark').build()

        then:
        result.output.contains('benchmark -')
    }
}
