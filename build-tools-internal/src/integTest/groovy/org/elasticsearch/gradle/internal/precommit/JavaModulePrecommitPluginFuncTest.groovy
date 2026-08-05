/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit

import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin
import org.gradle.testkit.runner.TaskOutcome

class JavaModulePrecommitPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    static final String ES_VERSION = VersionProperties.getElasticsearch()

    Class<? extends PrecommitPlugin> pluginClassUnderTest = JavaModulePrecommitPlugin.class

    def setup() {
        buildFile << """
        apply plugin:'java'

        version = '${ES_VERSION}'
        """
    }

    def "detects invalid module name prefix and reports structured problems"() {
        given:
        // module-version flag so the version check passes; bad prefix triggers name check
        buildFile << """
        tasks.named('compileJava') {
            options.compilerArgs.addAll(['--module-version', '${ES_VERSION}'])
        }
        """
        file("src/main/java/module-info.java").text = """
        module bad.module.name {
        }
        """
        file("src/main/java/bad/module/name/Dummy.java").text = """
        package bad.module.name;
        public class Dummy {}
        """

        when:
        def result = gradleRunner("validateModule").buildAndFail()

        then:
        result.task(":validateModule").outcome == TaskOutcome.FAILED

        and: "problems report contains java-module group"
        assertProblemsReportContains("java-module")
        assertProblemsReportContains("elasticsearch-build")
        assertProblemsReportContains("precommit")
        assertProblemsReportSeverity("invalid-module-name-prefix", "ERROR")
    }

    def "passes for valid module"() {
        given:
        buildFile << """
        tasks.named('compileJava') {
            options.compilerArgs.addAll(['--module-version', '${ES_VERSION}'])
        }
        """
        file("src/main/java/module-info.java").text = """
        module org.elasticsearch.testmodule {
        }
        """
        file("src/main/java/org/elasticsearch/testmodule/Dummy.java").text = """
        package org.elasticsearch.testmodule;
        public class Dummy {}
        """

        when:
        def result = gradleRunner("validateModule").build()

        then:
        result.task(":validateModule").outcome == TaskOutcome.SUCCESS
    }

    def "skips non-modular projects"() {
        given:
        file("src/main/java/org/acme/Foo.java").text = """
        package org.acme;
        public class Foo {}
        """

        when:
        def result = gradleRunner("validateModule").build()

        then:
        result.task(":validateModule").outcome == TaskOutcome.SUCCESS
    }

    def "problems have solutions in report"() {
        given:
        buildFile << """
        tasks.named('compileJava') {
            options.compilerArgs.addAll(['--module-version', '${ES_VERSION}'])
        }
        """
        file("src/main/java/module-info.java").text = """
        module bad.name {
        }
        """
        file("src/main/java/bad/name/Dummy.java").text = """
        package bad.name;
        public class Dummy {}
        """

        when:
        def result = gradleRunner("validateModule").buildAndFail()

        then:
        result.task(":validateModule").outcome == TaskOutcome.FAILED

        and: "each reported problem has a solution"
        def diagnostics = problemsReportDiagnostics()
        diagnostics.every { it.solutions != null && !it.solutions.isEmpty() }
    }
}
