/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.modules

import org.gradle.testkit.runner.TaskOutcome

class JavaModulesConsumerPluginFuncTest extends AbstractJavaModulesPluginFuncTest {

    def setup() {
        settingsFile << "include 'producing'\n"
        settingsFile << "include 'consuming'\n"
    }

    def "can compile against non module project internals"() {
        when:
        writeProducingJavaSource(new File(testProjectDir.root, 'producing'))
        writeConsumingInternalJavaSource(new File(testProjectDir.root, 'consuming'))
        writeModuleInfo(new File(testProjectDir.root, 'producing'))

        file('producing/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
            }
        """

        file('consuming/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules-consumer'
            }
            
            dependencies {
                implementation project(':producing')
            }
        """

        then:
        def result = gradleRunner("assemble").build()
        result.task(":consuming:compileJava").outcome == TaskOutcome.SUCCESS
    }

    def "can not compile against module project internals"() {
        when:
        writeProducingJavaSource(new File(testProjectDir.root, 'producing'))
        writeConsumingInternalJavaSource(new File(testProjectDir.root, 'consuming'))
        writeModuleInfo(new File(testProjectDir.root, 'producing'))

        file('producing/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules'
            }
        """

        file('consuming/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules-consumer'
            }
            
            dependencies {
                implementation project(':producing')
            }
        """

        then:
        def result = gradleRunner("assemble").buildAndFail()
        result.task(":consuming:compileJava").outcome == TaskOutcome.FAILED
        result.getOutput().contains("""ConsumingProducingInternal.java:4: error: package org.example.producing.impl does not exist
import org.example.producing.impl.SomethingInternal;
                                 ^""")
    }

    def "compiles against module api jar"() {
        when:
        writeProducingJavaSource(new File(testProjectDir.root, 'producing'))
        writeConsumingJavaSource(new File(testProjectDir.root, 'consuming'))
        writeModuleInfo(new File(testProjectDir.root, 'producing'))

        file('producing/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules'
            }
        """

        file('consuming/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules-consumer'
            }
            
            dependencies {
                implementation project(':producing')
            }
        """
        then:
        gradleRunner("assemble").build()
    }

    def "compiles against module transitive api dependencies"() {
        when:
        writeProducingJavaSource(new File(testProjectDir.root, 'producing'))
        writeConsumingJavaSource(new File(testProjectDir.root, 'consuming'))
        writeModuleInfo(new File(testProjectDir.root, 'producing'))

        file('producing/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules'
            }

            repositories {
                mavenCentral()
            }
            
            dependencies {
                api 'org.slf4j:slf4j-api:1.7.35'
            }
        """

        file('consuming/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules-consumer'
            }
            
            repositories {
                mavenCentral()
            }
            
            dependencies {
                implementation project(':producing')
            }
        """
        file("consuming/src/main/java/org/consuming/HelloWorld.java") << """
package org.consuming;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWorld {
    public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(HelloWorld.class);
    logger.info("Hello World");
  }
}
"""
        then:
        gradleRunner("assemble").build()
    }

    def "compiles against transitive non module projects"() {
        when:
        writeProducingJavaSource(new File(testProjectDir.root, 'producing'))
        writeConsumingJavaSource(new File(testProjectDir.root, 'consuming'))
        writeModuleInfo(new File(testProjectDir.root, 'producing'))

        settingsFile << "include 'transitive'\n"
        writeProducingJavaSource(new File(testProjectDir.root, 'transitive'))
        writeConsumingInternalJavaSource(new File(testProjectDir.root, 'consuming'), 'transitive')
        writeModuleInfo(new File(testProjectDir.root, 'transitive'))

        file('transitive/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
            }
        """

        file('producing/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules'
            }

            repositories {
                mavenCentral()
            }
            
            dependencies {
                api project(":transitive")
            }
        """

        file('consuming/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules-consumer'
            }
            
            repositories {
                mavenCentral()
            }
            
            dependencies {
                implementation project(':producing')
            }
        """
        then:
        gradleRunner("assemble").build()
    }

    def "compiles not against transitive module project internals"() {
        when:
        writeProducingJavaSource(new File(testProjectDir.root, 'producing'))
        writeConsumingJavaSource(new File(testProjectDir.root, 'consuming'))
        writeModuleInfo(new File(testProjectDir.root, 'producing'))

        settingsFile << "include 'transitive'\n"
        writeProducingJavaSource(new File(testProjectDir.root, 'transitive'))
        writeConsumingInternalJavaSource(new File(testProjectDir.root, 'consuming'), 'transitive')
        writeModuleInfo(new File(testProjectDir.root, 'transitive'))

        file('transitive/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules'
            }
        """

        file('producing/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules'
            }

            repositories {
                mavenCentral()
            }
            
            dependencies {
                api project(":transitive")
            }
        """

        file('consuming/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules-consumer'
            }
            
            repositories {
                mavenCentral()
            }
            
            dependencies {
                implementation project(':producing')
            }
        """
        then:
        def result = gradleRunner("assemble").buildAndFail()
        result.output.contains("""ConsumingTransitiveInternal.java:4: error: package org.example.transitive.impl does not exist
import org.example.transitive.impl.SomethingInternal;
                                  ^""")
    }
}
