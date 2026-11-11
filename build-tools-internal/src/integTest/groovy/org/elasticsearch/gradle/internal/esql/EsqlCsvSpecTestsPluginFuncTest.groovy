/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.esql

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome

class EsqlCsvSpecTestsPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends Plugin> pluginClassUnderTest = EsqlCsvSpecTestsPlugin

    def "generates one IT class per csv-spec file for each configured variant"() {
        given:
        def specDir = dir("spec")
        new File(specDir, "my_tests.csv-spec") << "// dummy spec\n"
        new File(specDir, "other_tests.csv-spec") << "// dummy spec\n"

        buildFile << """
            apply plugin: 'java'

            // The plugin wires csvSpecTest against javaRestTest; create a minimal one.
            sourceSets {
                javaRestTest {}
            }

            esqlCsvSpecTests {
                specFilesDir = file('spec')
                packageName = 'org.example.test'
                variant 'EsqlSpec', 'AbstractEsqlSpecIT'
            }
        """

        when:
        def result = gradleRunner('generateEsqlSpecTests').build()

        then:
        result.task(':generateEsqlSpecTests').outcome == TaskOutcome.SUCCESS
        file('build/generated-csv-spec-test-sources/java/org/example/test/EsqlSpecMyTestsIT.java').exists()
        file('build/generated-csv-spec-test-sources/java/org/example/test/EsqlSpecOtherTestsIT.java').exists()
    }

    def "generateEsqlSpecTests is up-to-date when inputs have not changed"() {
        given:
        def specDir = dir("spec")
        new File(specDir, "my_tests.csv-spec") << "// dummy spec\n"

        buildFile << """
            apply plugin: 'java'

            sourceSets {
                javaRestTest {}
            }

            esqlCsvSpecTests {
                specFilesDir = file('spec')
                packageName = 'org.example.test'
                variant 'EsqlSpec', 'AbstractEsqlSpecIT'
            }
        """

        when:
        gradleRunner('generateEsqlSpecTests').build()
        def result = gradleRunner('generateEsqlSpecTests').build()

        then:
        result.task(':generateEsqlSpecTests').outcome == TaskOutcome.UP_TO_DATE
    }

    def "generateEsqlSpecTests is loaded from build cache after clean"() {
        given:
        def specDir = dir("spec")
        new File(specDir, "my_tests.csv-spec") << "// dummy spec\n"

        buildFile << """
            apply plugin: 'java'

            sourceSets {
                javaRestTest {}
            }

            esqlCsvSpecTests {
                specFilesDir = file('spec')
                packageName = 'org.example.test'
                variant 'EsqlSpec', 'AbstractEsqlSpecIT'
            }
        """

        when:
        gradleRunner('generateEsqlSpecTests', '--build-cache').build()
        gradleRunner('clean').build()
        def result = gradleRunner('generateEsqlSpecTests', '--build-cache').build()

        then:
        result.task(':generateEsqlSpecTests').outcome == TaskOutcome.FROM_CACHE
    }
}
