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

class ElasticsearchJavaPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = ElasticsearchJavaPlugin.class

    def "compatibility options are resolved from from build params minimum runtime version"() {
        when:
        buildFile.text << """
        import org.elasticsearch.gradle.Architecture
        assert tasks.named('compileJava').get().sourceCompatibility == JavaVersion.VERSION_1_10.toString()
        assert tasks.named('compileJava').get().targetCompatibility == JavaVersion.VERSION_1_10.toString()
        """

        then:
        gradleRunner("help").build()
    }

    def "compile option --release is configured from targetCompatibility"() {
        when:
        buildFile.text << """
            tasks.named('compileJava').get().targetCompatibility = "1.10"
            afterEvaluate {
                assert tasks.named('compileJava').get().options.release.get() == 10
            }
        """
        then:
        gradleRunner("help").build()
    }
}
