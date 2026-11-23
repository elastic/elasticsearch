/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import org.apache.commons.io.FileUtils
import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.Unroll

class ElasticsearchJavadocPluginFuncTest extends AbstractGradleInternalPluginFuncTest {

    Class<? extends org.gradle.api.Plugin> pluginClassUnderTest = org.elasticsearch.gradle.internal.ElasticsearchJavadocPlugin

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
        options.contains("-linkoffline '$expectedLink' '../some-lib/build/docs/javadoc/'")

        where:
        version        | versionType | expectedLink
        '1.0'          | 'release'   | "https://artifacts.elastic.co/javadoc/org/acme/some-lib/$version"
        '1.0-SNAPSHOT' | 'snapshot'  | "https://snapshots.elastic.co/javadoc/org/acme/some-lib/$version"
    }

    def "javadoc with inter project linking is relocatable"() {
        given:
        def buildCacheDir = testProjectDir.newFolder("build-cache")
        def originalDir = testProjectDir.newFolder("original")
        def relocatedDir = testProjectDir.newFolder("relocated")
        someLibProject()
        someDependingLibProject()
        buildFile << """
            allprojects {
                version = '1.0'
            }
        """
        settingsFile << """
            buildCache {
                local {
                    directory = file('${buildCacheDir.absolutePath.replace('\\', '/')}')
                }
            }
        """
        copyProjectTo(originalDir)
        copyProjectTo(relocatedDir)

        when:
        gradleRunner(originalDir, ':some-depending-lib:javadoc', '--build-cache').build()
        def result = gradleRunner(relocatedDir, ':some-depending-lib:javadoc', '--build-cache').build()

        then:
        result.task(":some-lib:javadoc").outcome == TaskOutcome.FROM_CACHE
        result.task(":some-depending-lib:javadoc").outcome == TaskOutcome.FROM_CACHE
    }

    def "javadoc is not taken from the build cache when the linked version changes"() {
        given:
        someLibProject()
        someDependingLibProject()
        buildFile << """
            allprojects {
                version = providers.gradleProperty('acmeVersion').get()
                // the default title contains the version; keep it stable so only the links change
                tasks.withType(Javadoc).configureEach { title = 'Acme' }
            }
        """

        when:
        gradleRunner(':some-depending-lib:javadoc', '--build-cache', '-PacmeVersion=1.0').build()
        def result = gradleRunner('clean', ':some-depending-lib:javadoc', '--build-cache', '-PacmeVersion=2.0').build()

        then:
        // the upstream javadoc content does not depend on the version, only the links of the depending javadoc do
        result.task(":some-lib:javadoc").outcome == TaskOutcome.FROM_CACHE
        result.task(":some-depending-lib:javadoc").outcome == TaskOutcome.SUCCESS
        def options = normalized(file('some-depending-lib/build/tmp/javadoc/javadoc.options').text)
        options.contains("-linkoffline 'https://artifacts.elastic.co/javadoc/org/acme/some-lib/2.0' '../some-lib/build/docs/javadoc/'")
    }

    def "sources of shadowed dependencies are added to projects javadoc"() {
        given:
        settingsFile.text = ""
        someLibProject() << """version = 1.0"""
        subProject("some-depending-lib") {
            buildFile << """
                plugins {
                    id 'elasticsearch.java-doc'
                    id 'com.gradleup.shadow'
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
        options.contains("-linkoffline 'https://artifacts.elastic.co/javadoc/org/acme/some-lib/1.0' '../some-lib/build/docs/javadoc/'")
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
        options.contains("-linkoffline 'https://artifacts.elastic.co/javadoc/org/acme/some-lib/1.0' '../some-lib/build/docs/javadoc'") == false
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

    private File someDependingLibProject() {
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
    }

    /**
     * Copies the test project (without the copy targets themselves) into {@code targetDir} so the same build
     * can be run from different locations.
     */
    private void copyProjectTo(File targetDir) {
        def root = testProjectDir.root
        root.eachFile { f ->
            if (f.name in ['build-cache', 'original', 'relocated'] == false) {
                if (f.isDirectory()) {
                    FileUtils.copyDirectory(f, new File(targetDir, f.name))
                } else {
                    FileUtils.copyFileToDirectory(f, targetDir)
                }
            }
        }
    }
}
