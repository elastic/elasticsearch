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

class TestingConventionsPluginFuncTest extends AbstractGradlePrecommitPluginFuncTest {

    Class<? extends PrecommitPlugin> pluginClassUnderTest = TestingConventionsPrecommitPlugin.class

    def setup() {
        // default base class for unit tests
        testClass("org.apache.lucene.tests.util.LuceneTestCase") {
            """
            void testMe() {
            }
            """
        }
    }
    def "provides convention task for unit test sourceSet"() {
        given:
        buildFile << """
        apply plugin:'java'
        """
        testClass("org.acme.valid.SomeTests", "org.apache.lucene.tests.util.LuceneTestCase") {
            """
            void testMe() {
            }
            """
        }
        when:
        def result = gradleRunner("precommit").build();
        then:
        result.task(":testTestingConventions").outcome == TaskOutcome.SUCCESS
        result.task(":testingConventions").outcome == TaskOutcome.SUCCESS

        when:
        testClass("org.acme.InvalidTests") {
            """
            void testMe() {
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

    File testClass(String testClassName) {
        testClass(testClassName, {})
    }

    File testClass(String testClassName, String parent) {
        testClass(testClassName, parent, {})
    }

    File testClass(String testClassName, Closure<String> content) {
        testClass(testClassName, null, content)
    }

    File testClass(String testClassName, parent, Closure<String> content) {
        def testClassFile = file("src/test/java/${testClassName.replace('.', '/')}.java")
        def packageName = testClassName.substring(0, testClassName.lastIndexOf('.'))
        def className = testClassName.substring(testClassName.lastIndexOf('.') + 1)
        testClassFile << """
        package ${packageName};
        public class ${className} ${parent != null ? "extends $parent" : ""} {
            
            ${content.call()}
            
        }
        """
    }
}
