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
        settingsFile << """
gradleEnterprise {
    server = "https://gradle-enterprise.elastic.co"
}
        """
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
        def result = gradleRunner("assemble", "--scan").buildAndFail()
        result.task(":consuming:compileJava").outcome == TaskOutcome.FAILED
        result.getOutput().contains("""ConsumingInternal.java:4: error: package org.example.producing.impl does not exist
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
}
