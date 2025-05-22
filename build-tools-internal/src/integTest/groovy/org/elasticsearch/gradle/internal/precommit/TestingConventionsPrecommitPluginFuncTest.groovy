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
import org.elasticsearch.gradle.fixtures.LocalRepositoryFixture
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin
import org.gradle.testkit.runner.TaskOutcome
import org.junit.ClassRule
import spock.lang.Shared
import spock.lang.Unroll

class TestingConventionsPrecommitPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends PrecommitPlugin> pluginClassUnderTest = TestingConventionsPrecommitPlugin.class

    @Shared
    @ClassRule
    public LocalRepositoryFixture repository = new LocalRepositoryFixture()

    def setupSpec() {
        repository.generateJar('org.apache.lucene', 'tests.util', "1.0",
                "org.apache.lucene.tests.util.LuceneTestCase",
                "org.elasticsearch.test.ESSingleNodeTestCase",
                "org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase",
                "org.elasticsearch.test.AbstractMultiClustersTestCase"
        )
        repository.generateJar('junit', 'junit', "4.42",
                "org.junit.Assert", "org.junit.Test"
        )
    }

    def setup() {
        repository.configureBuild(buildFile)
    }

    def "skips convention check if no tests available"() {
        given:
        buildFile << """
        apply plugin:'java'
        """
        when:
        def result = gradleRunner("precommit").build()

        then:
        result.task(":testTestingConventions").outcome == TaskOutcome.NO_SOURCE
        result.task(":testingConventions").outcome == TaskOutcome.UP_TO_DATE
    }

    def "testing convention tasks are cacheable and uptodate"() {
        given:
        simpleJavaBuild()
        testClazz("org.acme.valid.SomeTests", "org.apache.lucene.tests.util.LuceneTestCase") {
            """
            public void testMe() {
            }
            """
        }
        when:
        gradleRunner("clean", "precommit", "--build-cache").build()
        def result = gradleRunner("clean", "precommit", "--build-cache").build()
        then:
        result.task(":testTestingConventions").outcome == TaskOutcome.FROM_CACHE
        result.task(":testingConventions").outcome == TaskOutcome.UP_TO_DATE

        when:
        result = gradleRunner("precommit").build()
        then:
        result.task(":testTestingConventions").outcome == TaskOutcome.UP_TO_DATE
        result.task(":testingConventions").outcome == TaskOutcome.UP_TO_DATE
    }

    def "checks base class convention"() {
        given:
        simpleJavaBuild()
        testClazz("org.acme.valid.SomeTests", "org.apache.lucene.tests.util.LuceneTestCase") {
            """
            public void testMe() {
            }
            """
        }
        when:
        def result = gradleRunner("precommit").build()
        then:
        result.task(":testTestingConventions").outcome == TaskOutcome.SUCCESS
        result.task(":testingConventions").outcome == TaskOutcome.SUCCESS

        when:
        testClazz("org.acme.InvalidTests") {
            """
            public void testMe() {
            }
            """
        }
        result = gradleRunner("precommit").buildAndFail()
        then:
        result.task(":testTestingConventions").outcome == TaskOutcome.FAILED

        assertOutputContains(result.getOutput(), """\
            * What went wrong:
            Execution failed for task ':testTestingConventions'.
            > A failure occurred while executing org.elasticsearch.gradle.internal.precommit.TestingConventionsCheckTask\$TestingConventionsCheckWorkAction
               > Following test classes do not extend any supported base class:
                 \torg.acme.InvalidTests""".stripIndent()
        )
    }

    def "checks naming convention"() {
        given:
        simpleJavaBuild()
        buildFile << """
        tasks.named('testTestingConventions').configure {
            suffix 'UnitTest'
        }
        """

        testClazz("org.acme.valid.SomeNameMissmatchingTest", "org.apache.lucene.tests.util.LuceneTestCase") {
            """
            public void testMe() {
            }
            """
        }

        testClazz("org.acme.valid.SomeMatchingUnitTest", "org.apache.lucene.tests.util.LuceneTestCase") {
            """
            public void testMe() {
            }
            """
        }

        when:
        def result = gradleRunner("precommit").buildAndFail()
        then:
        result.task(":testTestingConventions").outcome == TaskOutcome.FAILED
        assertOutputContains(result.getOutput(), """\
            * What went wrong:
            Execution failed for task ':testTestingConventions'.
            > A failure occurred while executing org.elasticsearch.gradle.internal.precommit.TestingConventionsCheckTask\$TestingConventionsCheckWorkAction
               > Following test classes do not match naming convention to use suffix 'UnitTest':
                 \torg.acme.valid.SomeNameMissmatchingTest""".stripIndent()
        )
    }

    def "provided base classes do not need match naming convention"() {
        given:
        simpleJavaBuild()
        buildFile << """
        tasks.named('testTestingConventions').configure {
            baseClass 'org.acme.SomeCustomTestBaseClass'
        }
        """

        testClazz("org.acme.SomeCustomTestBaseClass", "org.junit.Assert")
        testClazz("org.acme.valid.SomeNameMatchingTests", "org.acme.SomeCustomTestBaseClass") {
            """
            public void testMe() {
            }
            """
        }

        when:
        def result = gradleRunner("precommit").build()
        then:
        result.task(":testTestingConventions").outcome == TaskOutcome.SUCCESS
    }

    def "applies conventions on yaml-rest-test tests"() {
        given:
        buildApiRestrictionsDisabled = true
        clazz(dir('src/yamlRestTest/java'), "org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase")
        buildFile << """
        apply plugin:'elasticsearch.legacy-yaml-rest-test'

        dependencies {
            yamlRestTestImplementation "org.apache.lucene:tests.util:1.0"
            yamlRestTestImplementation "junit:junit:4.42"
        }
        """

        clazz(dir("src/yamlRestTest/java"), "org.acme.valid.SomeMatchingIT", "org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase") {
            """
            public void testMe() {
            }
            """
        }
        clazz(dir("src/yamlRestTest/java"), "org.acme.valid.SomeOtherMatchingIT", null) {
            """
            public void testMe() {
            }
            """
        }

        when:
        def result = gradleRunner("testingConventions").buildAndFail()
        then:
        result.task(":yamlRestTestTestingConventions").outcome == TaskOutcome.FAILED
        assertOutputContains(result.getOutput(), """\
            * What went wrong:
            Execution failed for task ':yamlRestTestTestingConventions'.
            > A failure occurred while executing org.elasticsearch.gradle.internal.precommit.TestingConventionsCheckTask\$TestingConventionsCheckWorkAction
               > Following test classes do not extend any supported base class:
                 \torg.acme.valid.SomeOtherMatchingIT""".stripIndent()
        )
    }

    @Unroll
    def "applies conventions on #sourceSetName tests"() {
        given:
        buildApiRestrictionsDisabled = pluginName.contains('legacy')
        clazz(dir("src/${sourceSetName}/java"), "org.elasticsearch.test.ESIntegTestCase")
        clazz(dir("src/${sourceSetName}/java"), "org.elasticsearch.test.rest.ESRestTestCase")
        buildFile << """
        import org.elasticsearch.gradle.internal.precommit.TestingConventionsCheckTask
        apply plugin:'$pluginName'

        dependencies {
            ${sourceSetName}Implementation "org.apache.lucene:tests.util:1.0"
            ${sourceSetName}Implementation "junit:junit:4.42"
        }
        tasks.withType(TestingConventionsCheckTask).configureEach {
            suffix 'IT'
            suffix 'Tests'
        }
        """

        clazz(dir("src/${sourceSetName}/java"), "org.acme.valid.SomeMatchingIT", "org.elasticsearch.test.ESIntegTestCase") {
            """
            public void testMe() {}
            """
        }

        clazz(dir("src/${sourceSetName}/java"), "org.acme.valid.SomeNonMatchingTest", "org.elasticsearch.test.ESIntegTestCase") {
            """
            public void testMe() {}
            """
        }

        when:
        def result = gradleRunner(taskName).buildAndFail()
        then:
        result.task(taskName).outcome == TaskOutcome.FAILED
        assertOutputContains(result.getOutput(), """\
            * What went wrong:
            Execution failed for task '${taskName}'.
            > A failure occurred while executing org.elasticsearch.gradle.internal.precommit.TestingConventionsCheckTask\$TestingConventionsCheckWorkAction
               > Following test classes do not match naming convention to use suffix 'IT' or 'Tests':
                 \torg.acme.valid.SomeNonMatchingTest""".stripIndent()
        )

        where:
        pluginName                              | taskName                                 | sourceSetName
        "elasticsearch.legacy-java-rest-test"   | ":javaRestTestTestingConventions"        | "javaRestTest"
        "elasticsearch.internal-cluster-test"   | ":internalClusterTestTestingConventions" | "internalClusterTest"
    }

    private void simpleJavaBuild() {
        buildFile << """
        apply plugin:'java'

        dependencies {
            testImplementation "org.apache.lucene:tests.util:1.0"
            testImplementation "junit:junit:4.42"
        }
        """
    }
}
