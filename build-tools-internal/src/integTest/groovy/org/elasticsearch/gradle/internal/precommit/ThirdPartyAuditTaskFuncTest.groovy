/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit

import net.bytebuddy.ByteBuddy
import net.bytebuddy.description.modifier.Ownership
import net.bytebuddy.description.modifier.Visibility
import net.bytebuddy.dynamic.DynamicType
import net.bytebuddy.implementation.FixedValue
import org.apache.logging.log4j.LogManager
import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.elasticsearch.gradle.internal.conventions.precommit.LicenseHeadersPrecommitPlugin
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome


import static org.elasticsearch.gradle.fixtures.TestClasspathUtils.setupJarJdkClasspath

class ThirdPartyAuditTaskFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends PrecommitPlugin> pluginClassUnderTest = ThirdPartyAuditPrecommitPlugin.class

    def setup() {
        buildFile << """
        import org.elasticsearch.gradle.internal.precommit.ThirdPartyAuditPrecommitPlugin
        import org.elasticsearch.gradle.internal.precommit.ThirdPartyAuditTask

        apply plugin:'java'

        group = 'org.elasticsearch'
        version = 'current'
        repositories {
          maven {
            name = "local-test"
            url = file("local-repo")
            metadataSources {
              artifact()
            }
          }
          mavenCentral()
        }

        tasks.named("thirdPartyAudit").configure {
          signatureFile = file('signature-file.txt')
        }
        """
    }

    def "ignores dependencies with org.elasticsearch"() {
        given:
        def group = "org.elasticsearch.gradle"
        generateDummyJars(group)
        setupJarJdkClasspath(dir('local-repo/org/elasticsearch/elasticsearch-core/current/'))
        file('signature-file.txt') << "@defaultMessage non-public internal runtime class"

        buildFile << """
            dependencies {
              jdkJarHell 'org.elasticsearch:elasticsearch-core:current'
              compileOnly "$group:broken-log4j:0.0.1"
              implementation "$group:dummy-io:0.0.1"
            }
            """
        when:
        def result = gradleRunner("thirdPartyAudit").build()
        then:
        result.task(":thirdPartyAudit").outcome == TaskOutcome.NO_SOURCE
        assertNoDeprecationWarning(result)
    }

    def "reports violations and ignores compile only"() {
        given:
        def group = "org.acme"
        generateDummyJars(group)

        file('signature-file.txt') << """@defaultMessage non-public internal runtime class
            java.io.**"""

        setupJarJdkClasspath(dir('local-repo/org/elasticsearch/elasticsearch-core/current/'))
        buildFile << """
            dependencies {
              jdkJarHell 'org.elasticsearch:elasticsearch-core:current'
              compileOnly "$group:broken-log4j:0.0.1"
              implementation "$group:dummy-io:0.0.1"
            }
            """
        when:
        def result = gradleRunner(":thirdPartyAudit").buildAndFail()
        then:
        result.task(":thirdPartyAudit").outcome == TaskOutcome.FAILED

        def output = normalized(result.getOutput())
        assertOutputContains(output, """\
            DEBUG: Classpath: [file:./build/precommit/thirdPartyAudit/thirdPartyAudit/]
            DEBUG: Detected Java 9 or later with module system.
            ERROR: Forbidden class/interface use: java.io.File [non-public internal runtime class]
            ERROR:   in org.acme.TestingIO (method declaration of 'getFile()')
            ERROR: Scanned 1 class file(s) for forbidden API invocations (in 0.00s), 1 error(s).
            ERROR: Check for forbidden API calls failed, see log.
            ==end of forbidden APIs==
            Classes with violations:
              * org.acme.TestingIO""".stripIndent())
        assertOutputMissing(output, "Missing classes:");
        assertNoDeprecationWarning(result);
    }

    def "reports missing classes for analysis"() {
        given:
        def group = "org.acme"
        generateDummyJars(group)

        file('signature-file.txt') << """\
            @defaultMessage non-public internal runtime class
            java.io.**"""

        setupJarJdkClasspath(dir('local-repo/org/elasticsearch/elasticsearch-core/current/'))
        buildFile << """
            dependencies {
              jdkJarHell 'org.elasticsearch:elasticsearch-core:current'
              compileOnly "$group:dummy-io:0.0.1"
              implementation "$group:broken-log4j:0.0.1"
            }
            """
        when:
        def result = gradleRunner(":thirdPartyAudit").buildAndFail()
        then:
        result.task(":thirdPartyAudit").outcome == TaskOutcome.FAILED

        def output = normalized(result.getOutput())
        assertOutputContains(output, """\
            DEBUG: Classpath: [file:./build/precommit/thirdPartyAudit/thirdPartyAudit/]
            DEBUG: Detected Java 9 or later with module system.
            DEBUG: Class 'org.apache.logging.log4j.LogManager' cannot be loaded (while looking up details about referenced class 'org.apache.logging.log4j.LogManager').
            WARNING: While scanning classes to check, the following referenced classes were not found on classpath (this may miss some violations):
            WARNING:   org.apache.logging.log4j.LogManager
            ==end of forbidden APIs==
            Missing classes:
              * org.apache.logging.log4j.LogManager""".stripIndent())
        assertOutputMissing(output, "Classes with violations:");
        assertNoDeprecationWarning(result);
    }

    def "reports jar hell with jdk"() {
        given:
        def group = "org.acme"
        generateDummyJars(group)

        file('signature-file.txt') << """\
            @defaultMessage non-public internal runtime class
            java.io.**
            """
        setupJarJdkClasspath(
                dir('local-repo/org/elasticsearch/elasticsearch-core/current/'),
                "> Audit of third party dependencies failed:" + "   Jar Hell with the JDK:" + "    * java.lang.String"
        )
        buildFile << """
            dependencies {
              jdkJarHell 'org.elasticsearch:elasticsearch-core:current'
              compileOnly "$group:dummy-io:0.0.1"
              implementation "$group:dummy-string:0.0.1"
            }
            """
        when:
        def result = gradleRunner(":thirdPartyAudit").buildAndFail()
        then:
        result.task(":thirdPartyAudit").outcome == TaskOutcome.FAILED

        def output = normalized(result.getOutput())
        assertOutputContains(output, """\
            Exception in thread "main" java.lang.IllegalStateException: > Audit of third party dependencies failed:   Jar Hell with the JDK:    * java.lang.String
            \tat org.elasticsearch.jdk.JdkJarHellCheck.main(Unknown Source)
            """.stripIndent())
        assertOutputContains(output, """\
            * What went wrong:
            Execution failed for task ':thirdPartyAudit'.
            > Audit of third party dependencies failed:
                Jar Hell with the JDK:
                *
            """.stripIndent())
        assertOutputMissing(output, "Classes with violations:");
        assertNoDeprecationWarning(result);
    }

    Object generateDummyJars(String groupId) {
        def baseGroupFolderPath = "local-repo/${groupId.replace('.', '/')}"
        DynamicType.Unloaded<?> stringDynamicType = new ByteBuddy().subclass(Object.class)
                .name("java.lang.String")
                .make()
        stringDynamicType.toJar(targetFile(dir("${baseGroupFolderPath}/dummy-string/0.0.1"),
                "dummy-string-0.0.1.jar"));

        DynamicType.Unloaded<?> ioDynamicType = new ByteBuddy().subclass(Object.class)
                .name("org.acme.TestingIO")
                .defineMethod("getFile", File.class, Visibility.PUBLIC, Ownership.MEMBER)
                .intercept(FixedValue.nullValue())
                .make()
        ioDynamicType.toJar(targetFile(dir("${baseGroupFolderPath}//dummy-io/0.0.1")
                , "dummy-io-0.0.1.jar"));

        DynamicType.Unloaded<?> loggingDynamicType = new ByteBuddy().subclass(Object.class)
                .name("org.acme.TestingLogging")
                .defineMethod("getLogManager", LogManager.class, Visibility.PUBLIC, Ownership.MEMBER)
                .intercept(FixedValue.nullValue())
                .make()
        loggingDynamicType.toJar(targetFile(dir("${baseGroupFolderPath}/broken-log4j/0.0.1/"), "broken-log4j-0.0.1.jar"))
    }

    GradleRunner gradleRunner(Object... arguments) {
        return super.gradleRunner(arguments).withEnvironment([RUNTIME_JAVA_HOME: System.getProperty("java.home")])
    }

    static File targetFile(File dir, String fileName) {
        new File(dir, fileName)
    }

}
