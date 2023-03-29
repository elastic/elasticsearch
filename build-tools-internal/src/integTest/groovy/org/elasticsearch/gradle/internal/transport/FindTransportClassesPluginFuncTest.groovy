/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.transport


import spock.lang.Shared

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.elasticsearch.gradle.fixtures.LocalRepositoryFixture
import org.elasticsearch.gradle.internal.TransportClassesCoveragePlugin
import org.gradle.testkit.runner.TaskOutcome
import org.junit.ClassRule

class FindTransportClassesPluginFuncTest extends AbstractGradleInternalPluginFuncTest {
    Class<? extends TransportClassesCoveragePlugin> pluginClassUnderTest = TransportClassesCoveragePlugin.class

    @Shared
    @ClassRule
    public LocalRepositoryFixture repository = new LocalRepositoryFixture()

    def setup() {
        buildFile << """
            apply plugin: 'java'
            apply plugin: 'elasticsearch.build'
            repositories {
                mavenCentral()
            }
            dependencies {
                testImplementation 'junit:junit:4.13.1'
                testImplementation 'org.hamcrest:hamcrest:2.1'
            }
            """

        //setup fake server & test framework
        subProject(":test:framework") << "apply plugin: 'elasticsearch.java'"
        subProject(":server") << "apply plugin: 'elasticsearch.java'"
        createClass("org.elasticsearch.common.io.stream.Writeable")

        // see https://github.com/gradle/gradle/issues/24172
        configurationCacheCompatible = false
    }

    def "no coverage is enforced for non transport classes"() {
        given:
        file("src/main/java/org/acme/NonWriteable.java") << """
            package org.acme;
            public class NonWriteable {
            }
        """
        file("src/test/java/org/acme/NonWriteableTests.java") << """
            package org.acme;
            import java.io.IOException;
            import org.junit.Test;

            public class NonWriteableTests  {
                @Test
                public void testA(){
                    var a = new NonWriteable();
                }
            }
        """
        when:
        def result = gradleRunner(":jacocoTestCoverageVerification").build()

        then:
        result.task(":jacocoTestCoverageVerification").outcome == TaskOutcome.SUCCESS
    }

    def "transport class have some coverage"() {
        given:
        file("src/main/java/org/acme/A.java") << """
            package org.acme;

            import org.elasticsearch.common.io.stream.Writeable;

            public class A extends Writeable {
            }
        """

        file("src/test/java/org/acme/ATests.java") << """
            package org.acme;
            import org.junit.Test;

            public class ATests  {
                @Test
                public void testA(){
                    var a = new A();
                }
            }
        """

        when:
        def result = gradleRunner(":jacocoTestCoverageVerification").build()

        then:
        result.task(":jacocoTestCoverageVerification").outcome == TaskOutcome.SUCCESS

        when:
        result = gradleRunner(":jacocoTestCoverageVerification").build()

        then:
        //TODO how do I make sure tests are not run again?
//        result.task(":test").outcome == TaskOutcome.UP_TO_DATE  is SUCCESS ..
        result.task(":findTransportClassesTask").outcome == TaskOutcome.UP_TO_DATE
//        result.task(":jacocoTestReport").outcome == TaskOutcome.UP_TO_DATE  is SUCCESS ..
//        result.task(":jacocoTestCoverageVerification").outcome == TaskOutcome.UP_TO_DATE  is SUCCESS ..
    }

    def "will fail when transport classes missing a test"() {
        given:
        file("src/main/java/org/acme/TransportClass.java") << """
            package org.acme;

            import org.elasticsearch.common.io.stream.Writeable;

            public class TransportClass extends Writeable {
            }
        """
        //TODO just making sure there is any test, but ideally projects without test dir should be good too..
        file("src/test/java/org/acme/ATests.java") << """
            package org.acme;
            import org.junit.Test;

            public class ATests  {
                @Test
                public void testA(){

                }
            }
        """
        when:
        def result = gradleRunner(":jacocoTestCoverageVerification").buildAndFail()

        then:
        result.task(":jacocoTestCoverageVerification").outcome == TaskOutcome.FAILED
        result.getOutput().contains("Rule violated for class org.acme.TransportClass")
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
