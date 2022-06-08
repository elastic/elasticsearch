/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit

import org.elasticsearch.gradle.fixtures.AbstractGradlePrecommitPluginFuncTest
import org.elasticsearch.gradle.fixtures.LocalRepositoryFixture
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin
import org.gradle.testkit.runner.TaskOutcome
import org.junit.ClassRule
import org.junit.rules.TemporaryFolder
import spock.lang.Shared

class TestingConventionsPrecommitPluginFuncTest extends AbstractGradlePrecommitPluginFuncTest {

    Class<? extends PrecommitPlugin> pluginClassUnderTest = TestingConventionsPrecommitPlugin.class


    @ClassRule
    @Shared
    public TemporaryFolder repoFolder = new TemporaryFolder()

    @Shared
    @ClassRule
    public LocalRepositoryFixture repository = new LocalRepositoryFixture(repoFolder)

    def setupSpec() {
        repository.generateJar('org.apache.lucene', 'tests.util', "1.0",
                "org.apache.lucene.tests.util.LuceneTestCase"
        )
        repository.generateJar('org.junit', 'junit', "4.42",
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

    def "testing convention plugin is configuration cache compliant"() {
        given:
        simpleJavaBuild()
        testClazz("org.acme.valid.SomeTests", "org.apache.lucene.tests.util.LuceneTestCase") {
            """
            public void testMe() {
            }
            """
        }
        when:
        def result = gradleRunner("precommit", "--configuration-cache").build()
        then:
        assertOutputContains(result.getOutput(), "0 problems were found storing the configuration cache.")

        when:
        result = gradleRunner("precommit", "--configuration-cache").build()
        then:
        assertOutputContains(result.getOutput(), "Configuration cache entry reused.")
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
            suffix = 'UnitTest'
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
        clazz(dir('src/yamlRestTest/java'), "org.elasticsearch.test.ESIntegTestCase")
        clazz(dir('src/yamlRestTest/java'), "org.elasticsearch.test.rest.ESRestTestCase")
        buildFile << """
        apply plugin:'elasticsearch.internal-yaml-rest-test'
        
        dependencies {
            yamlRestTestImplementation "org.apache.lucene:tests.util:1.0"
            yamlRestTestImplementation "org.junit:junit:4.42"
        }    
        """

        clazz(dir("src/yamlRestTest/java"), "org.acme.valid.SomeMatchingIT", "org.elasticsearch.test.ESIntegTestCase") {
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
    def "applies conventions on java-rest-test tests"() {
        given:
        clazz(dir('src/javaRestTest/java'), "org.elasticsearch.test.ESIntegTestCase")
        clazz(dir('src/javaRestTest/java'), "org.elasticsearch.test.rest.ESRestTestCase")
        buildFile << """
        apply plugin:'elasticsearch.internal-java-rest-test'
        
        dependencies {
            javaRestTestImplementation "org.apache.lucene:tests.util:1.0"
            javaRestTestImplementation "org.junit:junit:4.42"
        }    
        """

        clazz(dir("src/javaRestTest/java"), "org.acme.valid.SomeMatchingIT", "org.elasticsearch.test.ESIntegTestCase") {
            """
            public void testMe() {
            }
            """
        }

        clazz(dir("src/javaRestTest/java"), "org.acme.valid.SomeNonMatchingTest", "org.elasticsearch.test.ESIntegTestCase") {
            """
            public void testMe() {
            }
            """
        }

        when:
        def result = gradleRunner("testingConventions").buildAndFail()
        then:
        result.task(":javaRestTestTestingConventions").outcome == TaskOutcome.FAILED
        assertOutputContains(result.getOutput(), """\
            * What went wrong:
            Execution failed for task ':javaRestTestTestingConventions'.
            > A failure occurred while executing org.elasticsearch.gradle.internal.precommit.TestingConventionsCheckTask\$TestingConventionsCheckWorkAction
               > Following test classes do not match naming convention to use suffix 'IT':
                 \torg.acme.valid.SomeNonMatchingTest""".stripIndent()
        )
    }

    private void simpleJavaBuild() {
        buildFile << """
        apply plugin:'java'
                
        dependencies {
            testImplementation "org.apache.lucene:tests.util:1.0"
            testImplementation "org.junit:junit:4.42"
        }    
        """
    }
}
