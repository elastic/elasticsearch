/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.transport

import spock.lang.Ignore
import spock.lang.Shared

import org.elasticsearch.gradle.fixtures.AbstractJavaGradleFuncTest
import org.elasticsearch.gradle.fixtures.LocalRepositoryFixture
import org.elasticsearch.gradle.internal.coverage.TransportClassesCoveragePlugin
import org.gradle.testkit.runner.TaskOutcome
import org.junit.ClassRule

@Ignore
class TransportClassesCoveragePluginFuncTest extends AbstractJavaGradleFuncTest {

     Class<TransportClassesCoveragePlugin> getPluginClassUnderTest(){
         return TransportClassesCoveragePlugin.class
     }

    @Shared
    @ClassRule
    public LocalRepositoryFixture repository = new LocalRepositoryFixture()

    def setup() {
        buildFile << """
            import ${getPluginClassUnderTest().getName()}
            plugins {
              id 'elasticsearch.global-build-info'
              id 'base'
              id 'elasticsearch.transport-coverage'
            }
            """

        subProject(":aSubProject") << """
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

        createClass("aSubProject", "org.elasticsearch.common.io.stream.StreamInput")
        createClass("aSubProject", "org.elasticsearch.common.io.stream.StreamOutput")

        // see https://github.com/gradle/gradle/issues/24172
        configurationCacheCompatible = false
    }

    def "transport class have some coverage"() {
        given:
        file("aSubProject/src/main/java/org/acme/A.java") << """
            package org.acme;
            import org.elasticsearch.common.io.stream.StreamInput;
            import org.elasticsearch.common.io.stream.StreamOutput;

            public class A  {
                A(StreamInput input){
                }
                public void writeTo(StreamOutput output){
                }
            }
        """

        file("aSubProject/src/test/java/org/acme/ATests.java") << """
            package org.acme;
            import org.junit.Test;

            public class ATests  {
                @Test
                public void testA(){
                    var a = new A(null);
                    a.writeTo(null);
                }
            }
        """

        when:
        def result = gradleRunner(":transportMethodCoverageVerifier", "-Dtests.seed=default").build()

        then:
        result.task(":transportMethodCoverageVerifier").outcome == TaskOutcome.SUCCESS

        when:
        result = gradleRunner(":transportMethodCoverageVerifier", "-Dtests.seed=default").build()

        then:
        result.task(":test").outcome == TaskOutcome.UP_TO_DATE
        result.task(":jacocoTestReport").outcome == TaskOutcome.UP_TO_DATE
//        result.task(":transportMethodCoverageVerifier").outcome == TaskOutcome.UP_TO_DATE
    }

    def "will fail when transport classes missing a test"() {
        given:
        file("aSubProject/src/main/java/org/acme/TransportClass.java") << """
            package org.acme;

            import org.elasticsearch.common.io.stream.StreamInput;
            import org.elasticsearch.common.io.stream.StreamOutput;

            public class TransportClass  {
                TransportClass(StreamInput input){
                }
                public void writeTo(StreamOutput output){
                }
            }
        """
        //TODO just making sure there is any test to trigger jacocoReport, but ideally projects without test dir should be good too..
        file("aSubProject/src/test/java/org/acme/ATests.java") << """
            package org.acme;
            import org.junit.Test;

            public class ATests  {
                @Test
                public void testA(){

                }
            }
        """
        when:
        def result = gradleRunner(":transportMethodCoverageVerifier").buildAndFail()

        then:
        result.task(":transportMethodCoverageVerifier").outcome == TaskOutcome.FAILED
        result.getOutput().contains(
            "Transport protocol code, using org.elasticsearch.common.io.stream.StreamInput/Output needs more test coverage."
        )
        result.getOutput().contains("Minimum coverage")
        result.getOutput().contains("Class org/acme/TransportClass coverage 0.0")
    }
    private File createClass(String parent , String classWithPackage) {
        def filePath = classWithPackage.replace('.', '/')
        def packageName = classWithPackage.substring(0, classWithPackage.lastIndexOf('.'))
        def className = classWithPackage.substring(classWithPackage.lastIndexOf('.') + 1)
        file(parent+ "/src/main/java/" + filePath + ".java") << """
            package %s;
            public abstract class %s { }
        """.formatted(packageName, className)
    }
}
