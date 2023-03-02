/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport

import spock.lang.Shared

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.elasticsearch.gradle.fixtures.LocalRepositoryFixture
import org.gradle.testkit.runner.TaskOutcome
import org.junit.ClassRule

import java.nio.file.Files

class TransportTestExistPluginFuncTest extends AbstractGradleInternalPluginFuncTest {
    Class<? extends TransportTestExistPrecommitPlugin> pluginClassUnderTest = TransportTestExistPrecommitPlugin.class

    @Shared
    @ClassRule
    public LocalRepositoryFixture repository = new LocalRepositoryFixture()

    def setup() {
        configurationCacheCompatible = false
        buildFile << """
            apply plugin: 'java'
            apply plugin: 'elasticsearch.build'
            repositories {
                mavenCentral()
            }
            """

        //setup fake server & test framework
        subProject(":test:framework") << "apply plugin: 'elasticsearch.java'"
        subProject(":server") << "apply plugin: 'elasticsearch.java'"
        createClass("org.elasticsearch.common.io.stream.Writeable")
        createClass("org.elasticsearch.test.AbstractWireTestCase")
        createClass("org.elasticsearch.test.AbstractQueryTestCase")
        createClass("org.elasticsearch.search.aggregations.BaseAggregationTestCase")
        createClass("org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase")
        createClass("org.elasticsearch.test.AbstractQueryVectorBuilderTestCase")
    }

    def "can scan non transport classes"() {
        given:
        file("src/main/java/org/acme/NonWriteable.java") << """
            package org.acme;
            public class NonWriteable {
            }
        """

        when:
        def result = gradleRunner(":transportTestExistCheck").build()

        then:
        result.task(":transportTestExistCheck").outcome == TaskOutcome.SUCCESS
    }

    def "can scan transport classes"() {
        given:
        file("src/main/java/org/acme/A.java") << """
            package org.acme;

            import org.elasticsearch.common.io.stream.Writeable;

            public class A extends Writeable {
            }
        """

        file("src/test/java/org/acme/ATests.java") << """
            package org.acme;
            import org.elasticsearch.test.AbstractWireTestCase;
            import java.io.IOException;

            public class ATests extends AbstractWireTestCase {
            }
        """

        when:
        def result = gradleRunner(":transportTestExistCheck").build()

        then:
        result.task(":transportTestExistCheck").outcome == TaskOutcome.SUCCESS
    }

    def "can fail when transport classes missing a test"() {
        given:
        file("src/main/java/org/acme/MissingTest.java") << """
            package org.acme;

            import org.elasticsearch.common.io.stream.Writeable;

            public class MissingTest extends Writeable {
            }
        """

        when:
        def result = gradleRunner(":transportTestExistCheck").buildAndFail()

        then:
        result.task(":transportTestExistCheck").outcome == TaskOutcome.FAILED
        result.getOutput().contains("There are 1 missing tests for classes")
        result.getOutput().contains("org.acme.MissingTest")
    }

    private File createClass(String classWithPackage) {
        def filePath = classWithPackage.replace('.','/')
        def packageName = classWithPackage.substring(0,classWithPackage.lastIndexOf('.'))
        def className = classWithPackage.substring(classWithPackage.lastIndexOf('.')+1)
        file("src/main/java/" + filePath + ".java") << """
            package %s;
            public abstract class %s { }
        """.formatted(packageName, className)
    }
}
