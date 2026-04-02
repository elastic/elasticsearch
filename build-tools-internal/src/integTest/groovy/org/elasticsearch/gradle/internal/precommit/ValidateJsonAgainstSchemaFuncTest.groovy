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

class ValidateJsonAgainstSchemaFuncTest extends AbstractGradleInternalPluginFuncTest {

    // ValidateRestSpecPlugin is not a PrecommitPlugin, so we use a dummy to satisfy
    // AbstractGradleInternalPluginFuncTest. The actual task is registered manually.
    Class<? extends PrecommitPlugin> pluginClassUnderTest = ForbiddenPatternsPrecommitPlugin.class

    def setup() {
        buildFile << """
        import org.elasticsearch.gradle.internal.precommit.ValidateJsonAgainstSchemaTask

        apply plugin:'java'
        """
    }

    def "detects schema violations and reports structured problems"() {
        given:
        def schemaFile = file("schema.json")
        schemaFile.text = '''{
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["name", "url"],
            "properties": {
                "name": { "type": "string" },
                "url": { "type": "string" }
            }
        }'''

        def validFile = file("src/main/resources/rest-api-spec/api/valid.json")
        validFile.text = '{"name": "test", "url": "http://example.com"}'

        def invalidFile = file("src/main/resources/rest-api-spec/api/invalid.json")
        invalidFile.text = '{"name": 123}'

        buildFile << """
        tasks.register("validateJson", ValidateJsonAgainstSchemaTask) {
            inputFiles = files("src/main/resources/rest-api-spec/api")
            jsonSchema = file("schema.json")
            report = file("\${buildDir}/reports/validateJson.txt")
        }
        """

        when:
        def result = gradleRunner("validateJson").buildAndFail()

        then:
        result.task(":validateJson").outcome == TaskOutcome.FAILED

        and: "problems report contains json-validation group"
        assertProblemsReportContains("json-validation")
        assertProblemsReportContainsProblem("schema-violation")
        assertProblemsReportSeverity("schema-violation", "ERROR")
    }

    def "passes with valid JSON files"() {
        given:
        def schemaFile = file("schema.json")
        schemaFile.text = '''{
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": { "type": "string" }
            }
        }'''

        def validFile = file("src/main/resources/rest-api-spec/api/valid.json")
        validFile.text = '{"name": "test"}'

        buildFile << """
        tasks.register("validateJson", ValidateJsonAgainstSchemaTask) {
            inputFiles = files("src/main/resources/rest-api-spec/api")
            jsonSchema = file("schema.json")
            report = file("\${buildDir}/reports/validateJson.txt")
        }
        """

        when:
        def result = gradleRunner("validateJson").build()

        then:
        result.task(":validateJson").outcome == TaskOutcome.SUCCESS
    }

    def "problems have solutions in report"() {
        given:
        def schemaFile = file("schema.json")
        schemaFile.text = '''{
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": { "type": "string" }
            }
        }'''

        def invalidFile = file("src/main/resources/rest-api-spec/api/bad.json")
        invalidFile.text = '{"name": 42}'

        buildFile << """
        tasks.register("validateJson", ValidateJsonAgainstSchemaTask) {
            inputFiles = files("src/main/resources/rest-api-spec/api")
            jsonSchema = file("schema.json")
            report = file("\${buildDir}/reports/validateJson.txt")
        }
        """

        when:
        def result = gradleRunner("validateJson").buildAndFail()

        then:
        result.task(":validateJson").outcome == TaskOutcome.FAILED

        and: "each reported problem has a solution"
        def diagnostics = problemsReportDiagnostics()
        diagnostics.every { it.solutions != null && !it.solutions.isEmpty() }
    }
}
