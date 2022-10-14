/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal

import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.fixtures.AbstractJavaGradleFuncTest
import org.gradle.internal.os.OperatingSystem
import org.gradle.testkit.runner.TaskOutcome
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.objectweb.asm.ClassReader
import org.objectweb.asm.tree.ClassNode

import java.nio.file.Files

class ElasticsearchJavaModulePathPluginFuncTest extends AbstractJavaGradleFuncTest {

    public static final GString JAVA_BASE_MODULE = "java.base:${System.getProperty("java.version")}"

    public static final String ES_VERSION = VersionProperties.getElasticsearch()

    public static final String COMPILE_JAVA_CONFIG = """
        tasks.named('compileJava').configure {
            doLast {
                def sep = org.elasticsearch.gradle.OS.current() == org.elasticsearch.gradle.OS.WINDOWS ? ':' : ';'
                println "COMPILE_JAVA_COMPILER_ARGS " + options.allCompilerArgs.join(sep)
                println "COMPILE_JAVA_CLASSPATH "  + classpath.asPath
            }
        }
    """

    @Rule
    TemporaryFolder rootBuild = new TemporaryFolder()

    def setup() {
        clazz("org.acme.JavaMainClass")
        subProject("some-lib") << """
            plugins {
                id 'java-library'
                id 'elasticsearch.java-module'
            }

            dependencies {
                api project(":some-other-lib")
            }
        """
        subProject("some-other-lib") << """
            plugins {
                id 'java-library'
                id 'elasticsearch.java-module'
            }
        """
        buildFile << """
            plugins {
                id 'java'
                id 'elasticsearch.java-module'
            }

            allprojects {
                version = '1.2.3'
                group = 'test'
            }

            dependencies {
                implementation project('some-lib')
            }

            $COMPILE_JAVA_CONFIG
        """
    }

    def "non module projects with non module dependencies"() {
        when:
        def result = gradleRunner('compileJava').build()
        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS

        assertModulePathClasspath([], normalized(result.output))
        assertCompileClasspath(['./some-lib/build/classes/java/main', './some-other-lib/build/classes/java/main'], normalized(result.output))
    }

    def "non module project with direct module dependency"() {
        given:
        file('some-lib/src/main/java/module-info.java') << """
        module someLibModule {
        }
        """
        when:
        def result = gradleRunner('compileJava').build()
        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS

        assertModulePathClasspath(['./some-lib/build/classes/java/main', './some-other-lib/build/classes/java/main'], normalized(result.output))
        assertCompileClasspath([], normalized(result.output))
        file('build/classes/java/main/module-info.class').exists() == false
    }

    def "module project with non module dependencies"() {
        given:
        file('src/main/java/module-info.java') << """
        module rootModule {
        }
        """
        when:
        def result = gradleRunner('compileJava').build()
        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS

        assertModulePathClasspath(['./some-lib/build/classes/java/main', './some-other-lib/build/classes/java/main'], normalized(result.output))
        assertCompileClasspath([], normalized(result.output))
        file('build/classes/java/main/module-info.class').exists()
        assertModuleInfo(file('build/classes/java/main/module-info.class'), 'rootModule', [JAVA_BASE_MODULE])
    }

    def "module project with module dependencies"() {
        given:
        file('some-other-lib/src/main/java/module-info.java') << """
        module someOtherLibModule {
        }
        """
        file('some-lib/src/main/java/module-info.java') << """
        module someLibModule {
            requires someOtherLibModule;
        }
        """
        file('src/main/java/module-info.java') << """
        module rootModule {
            requires someLibModule;
        }
        """
        when:
        def result = gradleRunner('compileJava').build()
        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS

        assertModulePathClasspath(['./some-lib/build/classes/java/main', './some-other-lib/build/classes/java/main'], normalized(result.output))
        assertCompileClasspath([], normalized(result.output))
        file('build/classes/java/main/module-info.class').exists()
        file('some-lib/build/classes/java/main/module-info.class').exists()
        file('some-other-lib/build/classes/java/main/module-info.class').exists()
        assertModuleInfo(file('build/classes/java/main/module-info.class'), 'rootModule', [JAVA_BASE_MODULE, 'someLibModule:'+ES_VERSION])
    }

    def "module project with transitive module dependency"() {
        given:
        file('src/main/java/module-info.java') << """
        module rootModule {
        }
        """
        file('some-other-lib/src/main/java/module-info.java') << """
        module someOtherLibModule {
        }
        """

        when:
        def result = gradleRunner('compileJava').build()
        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS

        assertModulePathClasspath(['./some-lib/build/classes/java/main', './some-other-lib/build/classes/java/main'], normalized(result.output))
        assertCompileClasspath([], normalized(result.output))
        file('build/classes/java/main/module-info.class').exists()
        assertModuleInfo(file('build/classes/java/main/module-info.class'), 'rootModule', [JAVA_BASE_MODULE])
    }

    def "included build with non module dependencies"() {
        given:
        file(rootBuild.root, 'settings.gradle') << """
        includeBuild '${projectDir.path.replace('\\', '\\\\')}'
        """

        file(rootBuild.root, 'build.gradle') << """
            plugins {
                id 'java'
                id 'elasticsearch.java-module'
            }

            dependencies {
                implementation 'test:some-lib:1.2.3'
            }

            $COMPILE_JAVA_CONFIG
        """

        and:
        writeClazz('org.parent.Main', null, file(rootBuild.root, "src/main/java/org/parent/Main.java"), null)

        when:
        def result = gradleRunner(rootBuild.root, "compileJava").build()

        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS

        assertModulePathClasspath([], normalized(result.output))
        assertCompileClasspath(['./some-lib/build/classes/java/main', './some-other-lib/build/classes/java/main'], normalized(result.output))
    }

    def "included build with module dependencies"() {
        given:
        file(rootBuild.root, 'settings.gradle') << """
        includeBuild '${projectDir.path.replace('\\', '\\\\')}'
        """

        file(rootBuild.root, 'build.gradle') << """
            plugins {
                id 'java'
                id 'elasticsearch.java-module'
            }

            dependencies {
                implementation 'test:some-lib:1.2.3'
            }

            $COMPILE_JAVA_CONFIG
        """

        and:
        file('some-lib/src/main/java/module-info.java') << """
        module someLibModule {
        }
        """
        writeClazz('org.parent.Main', null, file(rootBuild.root, "src/main/java/org/parent/Main.java"), null)

        when:
        def result = gradleRunner(rootBuild.root, "compileJava").build()

        then:
        result.task(":compileJava").outcome == TaskOutcome.SUCCESS

        assertModulePathClasspath(['./some-lib/build/classes/java/main', './some-other-lib/build/classes/java/main'], normalized(result.output))
        assertCompileClasspath([], normalized(result.output))
    }

    private def assertModulePathClasspath(List<String> expectedEntries, String output) {
        def allArgs = output.find(/(?<=COMPILE_JAVA_COMPILER_ARGS ).*\n/).trim()
        if(allArgs.isEmpty()) {
            assert expectedEntries.size() == 0
        } else {
            def sep = OperatingSystem.current().isWindows() ? ':' : ';'
            def modulePathEntries = allArgs.find(/(?<=.*--module-path=)[^${sep}]*(?=${sep})?/)
            doClasspathAssertion(modulePathEntries, expectedEntries)
        }
        true
    }

    private def assertCompileClasspath(List<String> expectedEntries, String output) {
        def find = output.find(/(?<=COMPILE_JAVA_CLASSPATH ).*\n/).trim()
        doClasspathAssertion(find, expectedEntries)
    }

    private def doClasspathAssertion(String find, List<String> expectedEntries) {
        def foundEntries = find.trim().isEmpty() ? [] : find.split(File.pathSeparator)
        assert foundEntries.size() == expectedEntries.size()
        for (int i = 0; i < foundEntries.size(); i++) {
            assert foundEntries[i] == expectedEntries[i]
        }
        true
    }

    def assertModuleInfo(File moduleClassFile, String expectedModuleName, List<String> requiredModules) {
        try (InputStream is = Files.newInputStream(moduleClassFile.toPath())) {
            ClassReader classReader = new ClassReader(is);
            ClassNode classNode = new ClassNode();
            classReader.accept(classNode, ClassReader.EXPAND_FRAMES);

            assert classNode.module.name == expectedModuleName
            assert classNode.module.version == VersionProperties.elasticsearch
            assert classNode.module.requires.collect {it -> "${it.module}:${it.version}" } == requiredModules
            assert classNode.module.packages == null
            assert classNode.module.exports == null
        }
        true
    }

}
