/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.modules

import org.apache.logging.log4j.core.util.Assert
import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Assertions

import java.util.zip.ZipEntry
import java.util.zip.ZipFile

class JavaModulesPluginIntegTest extends AbstractGradleFuncTest {

    def "can read module export"() {
        when:
        writeProvidingJavaSource()
        writeModuleInfo()
        buildFile.text = """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules'
            }
        """

        then:
        gradleRunner("modulesApiJar").build()
        file('build/distributions/hello-world-api.jar').exists()
        assertApiJar(['org/example/server/api/Component.class'])
    }

    def "consuming projects can not compile against module internals"() {
        when:
        settingsFile << "include 'providing'\n"
        settingsFile << "include 'consuming'\n"
        settingsFile << """
gradleEnterprise {
    server = "https://gradle-enterprise.elastic.co"
}
        """
        writeProvidingJavaSource(new File(testProjectDir.root, 'providing'))
        writeConsumingInternalJavaSource(new File(testProjectDir.root, 'consuming'))
        writeModuleInfo(new File(testProjectDir.root, 'providing'))

        file('providing/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules'
            }
        """

        file('consuming/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules'
            }
            
            dependencies {
                implementation project(':providing')
            }
        """

        then:
//        def result = gradleRunner(":providing:outgoingVariants", "--scan").buildAndFail()
        def result = gradleRunner("assemble", "--scan").buildAndFail()
        result.task(":consuming:compileJava").outcome == TaskOutcome.FAILED
        result.getOutput().contains("""consuming/src/main/java/org/consuming/ConsumingInternal.java:4: error: package org.example.server.impl does not exist
import org.example.server.impl.SomethingInternal;
                              ^""")
        file('providing/build/distributions/providing-api.jar').exists()
    }

    def "consuming projects compile against api jar"() {
        when:
        settingsFile << "include 'providing'\n"
        settingsFile << "include 'consuming'\n"
        writeProvidingJavaSource(new File(testProjectDir.root, 'providing'))
        writeConsumingJavaSource(new File(testProjectDir.root, 'consuming'))
        writeModuleInfo(new File(testProjectDir.root, 'providing'))

        file('providing/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules'
            }
        """

        file('consuming/build.gradle') << """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules'
            }
            
            dependencies {
                implementation project(':providing')
            }
        """
        then:
        gradleRunner("assemble").build()
        file('providing/build/distributions/providing-api.jar').exists()
    }

    private boolean assertApiJar(List<String> expectedEntries) {
        ZipFile jar = new ZipFile(file('build/distributions/hello-world-api.jar'));
        Enumeration<ZipEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
            ZipEntry e = entries.nextElement();
            if (e.getName().endsWith(".class")) {
                Assertions.assertTrue(expectedEntries.remove(e.getName()))
            }
        }
        Assert.isEmpty(expectedEntries);
    }

    void writeModuleInfo(File root = testProjectDir.root) {
        file(root,'src/main/java/module-info.java') << """
module org.example.server {
    exports org.example.server.api;
    uses org.example.server.api.Component;
}
"""

    }

    void writeProvidingJavaSource(File root = testProjectDir.root) {
        file(root, 'src/main/java/org/example/server/impl/SomethingInternal.java') << """package org.example.server.impl;

public class SomethingInternal {

    public void doSomething() {
        System.out.println("Something internal");
    }
}
"""

        file(root,'src/main/java/org/example/server/api/Component.java') << """package org.example.server.api;

public class Component {

    public Component() {
    }
    
    public void doSomething() {
        new org.example.server.impl.SomethingInternal().doSomething();
    }

}
"""
    }


    void writeConsumingInternalJavaSource(File root = testProjectDir.root) {
        file(root,'src/main/java/org/consuming/ConsumingInternal.java') << """package org.consuming;

import org.example.server.api.Component;
import org.example.server.impl.SomethingInternal;

public class ConsumingInternal {
    Component c = new Component();
    
    public void run() {
       SomethingInternal i = new SomethingInternal();
       i.doSomething();
    }

}
"""

    }

    void writeConsumingJavaSource(File root = testProjectDir.root) {
        file(root,'src/main/java/org/consuming/Consuming.java') << """package org.consuming;

import org.example.server.api.Component;

public class Consuming {
    Component c = new Component();
    
    public void run() {
       c.doSomething();
    }

}
"""

    }
}
