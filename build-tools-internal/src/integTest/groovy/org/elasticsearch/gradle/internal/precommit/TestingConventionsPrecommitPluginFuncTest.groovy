/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit

import org.elasticsearch.gradle.fixtures.AbstractGradlePrecommitPluginFuncTest
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin
import org.gradle.testkit.runner.TaskOutcome

class TestingConventionsPrecommitPluginFuncTest extends AbstractGradlePrecommitPluginFuncTest {

    Class<? extends PrecommitPlugin> pluginClassUnderTest = TestingConventionsPrecommitPlugin.class

    def setup() {
        // default base class for unit tests
        clazz("org.apache.lucene.tests.util.LuceneTestCase")
        // used in assuming tests
        clazz("org.junit.Assert")
        clazz("org.junit.Test")
    }

    def "skips convention check if no tests available"() {
        given:
        buildFile << """
        apply plugin:'java'
        """

        when:
        def result = gradleRunner("precommit").build();
        then:
        result.task(":testTestingConventions").outcome == TaskOutcome.NO_SOURCE
        result.task(":testingConventions").outcome == TaskOutcome.UP_TO_DATE
    }

    def "testing convention tasks are cacheable and uptodate"() {
        given:
        buildFile << """
        apply plugin:'java'
        """

        testClazz("org.acme.valid.SomeTests", "org.apache.lucene.tests.util.LuceneTestCase") {
            """
            public void testMe() {
            }
            """
        }
        when:
        gradleRunner("clean", "precommit", "--build-cache").build();
        def result = gradleRunner("clean", "precommit", "--build-cache").build();
        then:
        result.task(":testTestingConventions").outcome == TaskOutcome.FROM_CACHE
        result.task(":testingConventions").outcome == TaskOutcome.UP_TO_DATE

        when:
        result = gradleRunner("precommit").build();
        then:
        result.task(":testTestingConventions").outcome == TaskOutcome.UP_TO_DATE
        result.task(":testingConventions").outcome == TaskOutcome.UP_TO_DATE
    }

    def "testing convention plugin is configuration cache compliant"() {
        given:
        buildFile << """
        apply plugin:'java'
        """

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
        result = gradleRunner("precommit", "--configuration-cache").build();
        then:
        assertOutputContains(result.getOutput(), "Configuration cache entry reused.")
    }

    def "checks base class convention"() {
        given:
        buildFile << """
        apply plugin:'java'
        """

        testClazz("org.acme.valid.SomeTests", "org.apache.lucene.tests.util.LuceneTestCase") {
            """
            public void testMe() {
            }
            """
        }
        when:
        def result = gradleRunner("precommit").build();
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
        result = gradleRunner("precommit").buildAndFail();
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

    def "checks naming convention convention"() {
        given:
        buildFile << """
        apply plugin:'java'
        
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
        def result = gradleRunner("precommit").buildAndFail();
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
}
