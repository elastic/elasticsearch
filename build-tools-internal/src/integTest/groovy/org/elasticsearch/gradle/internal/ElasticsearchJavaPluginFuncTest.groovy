/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest

class ElasticsearchJavaPluginFuncTest extends AbstractGradleFuncTest {

    def "compatibility options are resolved from from build params minimum runtime version"() {
        when:
        buildFile.text = """
        plugins {
          id 'elasticsearch.global-build-info'
        }
        import org.elasticsearch.gradle.Architecture
        import org.elasticsearch.gradle.internal.info.BuildParams
        BuildParams.init { it.setMinimumRuntimeVersion(JavaVersion.VERSION_1_10) }

        apply plugin:'elasticsearch.java'

        assert compileJava.sourceCompatibility == JavaVersion.VERSION_1_10.toString()
        assert compileJava.targetCompatibility == JavaVersion.VERSION_1_10.toString()
        """

        then:
        gradleRunner("help").build()
    }

    def "compile option --release is configured from targetCompatibility"() {
        when:
        buildFile.text = """
            plugins {
             id 'elasticsearch.java'
            }

            compileJava.targetCompatibility = "1.10"
            afterEvaluate {
                assert compileJava.options.release.get() == 10
            }
        """
        then:
        gradleRunner("help").build()
    }
}
