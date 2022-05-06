/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.Unroll

class ElasticsearchJavadocPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        addSubProject("some-lib") {
            buildFile << """
                plugins {
                    id 'elasticsearch.java-doc'
                    id 'java'
                }
                group = 'org.acme'
                """

            classFile('org.acme.Something') << """
                package org.acme;
                
                public class Something {
                }
            """
        }
        addSubProject("some-depending-lib") {
            buildFile << """               
                plugins {
                    id 'elasticsearch.java-doc'
                    id 'java'
                }
                group = 'org.acme.depending'
                
                dependencies {
                    implementation project(':some-lib')
                }
            """
            classFile('org.acme.depending.SomeDepending') << """
                package org.acme.depending;
                
                import org.acme.Something;
                
                public class SomeDepending {
                    public Something createSomething() {
                        return new Something();
                    }
                }
            """
        }
    }

    @Unroll
    def "#versionType created javadoc with inter project linking"() {
        given:
        buildFile << """
            allprojects {
                version = '$version'
            }
        """
        when:
        def result = gradleRunner(':some-depending-lib:javadoc').build()
        then:
        result.task(":some-lib:javadoc").outcome == TaskOutcome.SUCCESS
        result.task(":some-depending-lib:javadoc").outcome == TaskOutcome.SUCCESS

        def options = file('some-depending-lib/build/tmp/javadoc/javadoc.options').text
        options.contains('-notimestamp')
        options.contains('-quiet')
        options.contains("-linkoffline '$expectedLink' '${file('some-lib/build/docs/javadoc/').canonicalPath}/'")

        where:
        version        | versionType | expectedLink
        '1.0'          | 'release'   | "https://artifacts.elastic.co/javadoc/org/acme/some-lib/$version"
        '1.0-SNAPSHOT' | 'snapshot'  | "https://snapshots.elastic.co/javadoc/org/acme/some-lib/$version"
    }
}
