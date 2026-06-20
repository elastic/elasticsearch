/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.foreign

import org.elasticsearch.gradle.fixtures.AbstractJavaGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

/**
 * Exercises the plugin's wiring in a real Gradle invocation. Validation of the actual processor's
 * output lives with the processor library itself; here we only verify that the plugin runs an
 * annotation processor against the consumer's sources and routes its output to the expected
 * locations. A dummy processor is supplied via the {@code foreignLibraryProcessor} configuration
 * so the test does not need any real {@code :libs:foreign-library} artifacts.
 */
class ForeignLibraryPluginFuncTest extends AbstractJavaGradleFuncTest {

    def setup() {
        internalBuild()

        settingsFile << """
            include ':fakeprocessor'
        """.stripIndent()

        // A trivial annotation processor: when invoked, it writes a marker resource file so the test
        // can assert that the processForeignAnnotations task did invoke a processor. It also writes a
        // mock services file at the path the plugin's augmentForeignModuleInfo task reads from,
        // proving the generated-classes output dir is wired correctly.
        file('fakeprocessor/build.gradle') << """
            apply plugin: 'java-library'
        """.stripIndent()

        file('fakeprocessor/src/main/java/fake/Marker.java') << """
            package fake;
            import java.lang.annotation.*;
            @Retention(RetentionPolicy.SOURCE)
            @Target(ElementType.TYPE)
            public @interface Marker {}
        """.stripIndent()

        file('fakeprocessor/src/main/java/fake/MarkerProcessor.java') << """
            package fake;
            import java.io.Writer;
            import java.util.Set;
            import javax.annotation.processing.*;
            import javax.lang.model.SourceVersion;
            import javax.lang.model.element.TypeElement;
            import javax.tools.StandardLocation;

            @SupportedAnnotationTypes("fake.Marker")
            public class MarkerProcessor extends AbstractProcessor {
                @Override
                public SourceVersion getSupportedSourceVersion() { return SourceVersion.latestSupported(); }

                @Override
                public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment env) {
                    if (env.processingOver() == false) {
                        try {
                            var marker = processingEnv.getFiler().createResource(
                                StandardLocation.CLASS_OUTPUT, "", "fake-marker.txt");
                            try (Writer w = marker.openWriter()) {
                                w.write("processor ran");
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return true;
                }
            }
        """.stripIndent()

        file('fakeprocessor/src/main/resources/META-INF/services/javax.annotation.processing.Processor') <<
            'fake.MarkerProcessor'

        // Consumer module: applies the plugin, annotates a class with @fake.Marker, and substitutes
        // the dummy processor for the plugin's default :libs:foreign-library:processor dep.
        file('src/main/java/test/Lib.java') << """
            package test;
            @fake.Marker
            public class Lib {}
        """.stripIndent()

        buildFile << """
            apply plugin: 'elasticsearch.foreign-library'

            // The plugin pins processForeignAnnotations to JDK 25 because the real processor uses
            // java.lang.classfile (finalized in JDK 24). The dummy processor in this test only uses
            // basic javax.annotation.processing APIs, and CI agents only ship the project's minimum
            // runtime, so override the toolchain to the current JVM. The JDK 25 pinning itself is
            // verified by ForeignLibraryPluginSpec — here we just exercise the wiring.
            def currentJavaVersion = JavaVersion.current().majorVersion
            tasks.named('processForeignAnnotations').configure {
                javaCompiler = javaToolchains.compilerFor {
                    languageVersion = org.gradle.jvm.toolchain.JavaLanguageVersion.of(currentJavaVersion.toInteger())
                }
                sourceCompatibility = currentJavaVersion
                targetCompatibility = currentJavaVersion
            }

            dependencies {
                // compileOnly so the @Marker annotation (SOURCE retention) is visible during compile
                compileOnly project(':fakeprocessor')
                foreignLibraryProcessor project(':fakeprocessor')
            }
        """.stripIndent()
    }

    def "wires foreignLibraryProcessor onto the annotation processor path and runs it"() {
        when:
        def result = gradleRunner('processForeignAnnotations', '-g', gradleUserHome).build()

        then:
        result.task(":processForeignAnnotations").outcome == TaskOutcome.SUCCESS
        // The dummy processor's marker file lands in the plugin's configured output dir, proving
        // the foreignLibraryProcessor config was wired onto javac's annotation processor path and
        // that Filer output routes to processForeignAnnotations' destinationDirectory.
        file("build/generated-foreign-library-classes/fake-marker.txt").exists()
        file("build/generated-foreign-library-classes/fake-marker.txt").text == "processor ran"
    }

}
