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

    @Unroll
    def "#versionType created javadoc with inter project linking"() {
        given:
        someLibProject()
        subProject("some-depending-lib") {
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

        def options = normalized(file('some-depending-lib/build/tmp/javadoc/javadoc.options').text)
        options.contains('-notimestamp')
        options.contains('-quiet')
        options.contains("-linkoffline '$expectedLink' './some-lib/build/docs/javadoc/'")

        where:
        version        | versionType | expectedLink
        '1.0'          | 'release'   | "https://artifacts.elastic.co/javadoc/org/acme/some-lib/$version"
        '1.0-SNAPSHOT' | 'snapshot'  | "https://snapshots.elastic.co/javadoc/org/acme/some-lib/$version"
    }

    def "sources of shadowed dependencies are added to projects javadoc"() {
        given:
        someLibProject() << """version = 1.0"""
        subProject("some-depending-lib") {
            buildFile << """               
                plugins {
                    id 'elasticsearch.java-doc'
                    id 'com.github.johnrengelman.shadow' version '7.1.2'
                    id 'java'
                }
                group = 'org.acme.depending'
                
                dependencies {
                    implementation project(':some-lib')
                    shadow project(':some-shadowed-lib')
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
            classFile('org.acme.depending.SomeShadowedDepending') << """
                package org.acme.depending;
                
                import org.acme.shadowed.Shadowed;
                
                public class SomeShadowedDepending {
                    public Shadowed createShadowed() {
                        return new Shadowed();
                    }
                }
            """
        }
        subProject("some-shadowed-lib") {
            buildFile << """
                plugins {
                    id 'elasticsearch.java-doc'
                    id 'java'
                }
                group = 'org.acme.shadowed'
            """
            classFile('org.acme.shadowed.Shadowed') << """
                package org.acme.shadowed;
                
                public class Shadowed {
                }
            """
        }
        when:
        def result = gradleRunner(':some-depending-lib:javadoc').build()

        then:
        def options = normalized(file('some-depending-lib/build/tmp/javadoc/javadoc.options').text)
        options.contains('-notimestamp')
        options.contains('-quiet')

        // normal dependencies handles as usual
        result.task(":some-lib:javadoc").outcome == TaskOutcome.SUCCESS
        options.contains("-linkoffline 'https://artifacts.elastic.co/javadoc/org/acme/some-lib/1.0' './some-lib/build/docs/javadoc/'")
        file('some-depending-lib/build/docs/javadoc/org/acme/Something.html').exists() == false

        // source of shadowed dependencies are inlined
        result.task(":some-shadowed-lib:javadoc") == null
        file('some-depending-lib/build/docs/javadoc/org/acme/shadowed/Shadowed.html').exists()
        normalized(file('some-depending-lib/build/docs/javadoc/element-list').text) == 'org.acme.depending\norg.acme.shadowed'
    }

    def "ignores skipped javadocs of dependent projects"() {
        given:
        someLibProject() << """
            version = '1.0'
            tasks.named("javadoc").configure { enabled = false }
        """
        subProject("some-depending-lib") {
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
        when:
        def result = gradleRunner(':some-depending-lib:javadoc').build()
        then:
        result.task(":some-lib:javadoc").outcome == TaskOutcome.SKIPPED
        result.task(":some-depending-lib:javadoc").outcome == TaskOutcome.SUCCESS

        def options = normalized(file('some-depending-lib/build/tmp/javadoc/javadoc.options').text)
        options.contains('-notimestamp')
        options.contains('-quiet')
        options.contains("-linkoffline 'https://artifacts.elastic.co/javadoc/org/acme/some-lib/1.0' './some-lib/build/docs/javadoc'") == false
    }

    def "ensures module dependency in javadoc of projects"() {
        given:
        subProject("foo-app") {
            buildFile << """
                plugins {
                    id 'java'
                    id 'elasticsearch.java-module'
                    id 'elasticsearch.java-doc'
                }
                group = 'org.example.foo'
                dependencies {
                    implementation project(':bar-lib')
                }
                """
            classFile('module-info') << """
                /** The foo app module. */
                module foo.app {
                  requires bar.lib;
                }
                """
            classFile('org.example.foo.Foo') << """
                package org.example.foo;

                /** The Foo app. */
                public class Foo {
                    /** The Foo method for calculating sums. */
                    public int calculateSum(int v1, int v2) {
                        return org.example.bar.BarMathUtils.sum(v1, v2);
                    }
                }
                """
        }
        subProject("bar-lib") {
            buildFile << """
                plugins {
                    id 'java'
                    id 'elasticsearch.java-doc'
                    id 'elasticsearch.java-module'

                }
                group = 'org.example.bar'
                """
            classFile('module-info') << """
                /** The bar lib module. */
                module bar.lib {
                    exports org.example.bar;
                }
                """
            classFile('org.example.bar.BarMathUtils') << """
                package org.example.bar;

                /** The Bar Math Utilities. */
                public final class BarMathUtils<T, R> {
                    /** Returns the sum of the given values. */
                    public static int sum(int x, int y) { return x + y; }
                }
                """
        }
        when:
        def result = gradleRunner(':foo-app:javadoc').build()
        then:
        result.task(":foo-app:javadoc").outcome == TaskOutcome.SUCCESS
        result.task(":bar-lib:javadoc").outcome == TaskOutcome.SUCCESS

        def options = normalized(file('foo-app/build/tmp/javadoc/javadoc.options').text)
        options.contains('--module-path')
    }

    String normalized(String input) {
        return super.normalized(input.replace("\\\\", "/"))
    }

    private File someLibProject() {
        subProject("some-lib") {
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
    }
}
