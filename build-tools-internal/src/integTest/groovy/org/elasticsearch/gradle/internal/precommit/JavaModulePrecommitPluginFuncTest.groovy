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

    def "detects explicit bundled module not reachable from module graph"() {
        given:
        // Simulate a plugin project: add a resolveableCompileOnly configuration (normally applied
        // by BasePluginBuildPlugin via CompileOnlyResolvePlugin) so the plugin wires bundledClasspath.
        buildFile << """
        configurations {
            resolveableCompileOnly {
                canBeResolved = true
                canBeConsumed = false
                extendsFrom configurations.compileOnly
            }
        }
        tasks.named('compileJava') {
            options.compilerArgs.addAll(['--module-version', '${ES_VERSION}'])
        }
        dependencies {
            // An explicit-module JAR built by the sibling subproject; it is bundled (implementation)
            // but intentionally omitted from module-info requires to trigger the check.
            implementation project(':mylib')
        }
        """
        settingsFile << "include ':mylib'"
        file("mylib/build.gradle").text = "plugins { id 'java' }"
        file("mylib/src/main/java/module-info.java").text = "module com.example.mylib { }"
        file("mylib/src/main/java/com/example/mylib/Lib.java").text = """
        package com.example.mylib;
        public class Lib {}
        """
        file("src/main/java/module-info.java").text = """
        module org.elasticsearch.testmodule {
            // intentionally missing: requires com.example.mylib;
        }
        """
        file("src/main/java/org/elasticsearch/testmodule/Dummy.java").text = """
        package org.elasticsearch.testmodule;
        public class Dummy {}
        """

        when:
        def result = gradleRunner("validateModule").buildAndFail()

        then:
        result.task(":validateModule").outcome == TaskOutcome.FAILED

        and: "problems report flags the unreachable bundled module"
        assertProblemsReportContains("unreachable-bundled-module")
        assertProblemsReportSeverity("unreachable-bundled-module", "ERROR")
    }

    def "passes when explicit bundled module is declared in requires"() {
        given:
        buildFile << """
        configurations {
            resolveableCompileOnly {
                canBeResolved = true
                canBeConsumed = false
                extendsFrom configurations.compileOnly
            }
        }
        tasks.named('compileJava') {
            options.compilerArgs.addAll(['--module-version', '${ES_VERSION}'])
        }
        dependencies {
            implementation project(':mylib')
        }
        """
        settingsFile << "include ':mylib'"
        file("mylib/build.gradle").text = "plugins { id 'java' }"
        file("mylib/src/main/java/module-info.java").text = "module com.example.mylib { }"
        file("mylib/src/main/java/com/example/mylib/Lib.java").text = """
        package com.example.mylib;
        public class Lib {}
        """
        file("src/main/java/module-info.java").text = """
        module org.elasticsearch.testmodule {
            requires com.example.mylib;
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

    def "skips resolution check for non-plugin modular projects (no resolveableCompileOnly)"() {
        given:
        // No resolveableCompileOnly → bundledClasspath stays null → resolution check is skipped.
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

    def "allowedUnreachable suppresses the check for a named module"() {
        given:
        buildFile << """
        configurations {
            resolveableCompileOnly {
                canBeResolved = true
                canBeConsumed = false
                extendsFrom configurations.compileOnly
            }
        }
        tasks.named('compileJava') {
            options.compilerArgs.addAll(['--module-version', '${ES_VERSION}'])
        }
        dependencies {
            implementation project(':mylib')
        }
        tasks.named('validateModule') {
            allowedUnreachable.add('com.example.mylib')
        }
        """
        settingsFile << "include ':mylib'"
        file("mylib/build.gradle").text = "plugins { id 'java' }"
        file("mylib/src/main/java/module-info.java").text = "module com.example.mylib { }"
        file("mylib/src/main/java/com/example/mylib/Lib.java").text = """
        package com.example.mylib;
        public class Lib {}
        """
        file("src/main/java/module-info.java").text = """
        module org.elasticsearch.testmodule {
            // com.example.mylib intentionally omitted but suppressed via allowedUnreachable
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
