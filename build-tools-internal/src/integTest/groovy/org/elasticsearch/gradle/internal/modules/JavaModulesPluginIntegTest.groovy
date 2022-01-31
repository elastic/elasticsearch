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
        gradleRunner("modulesApiJar").forwardOutput().build()
        file('build/distributions/hello-world-api.jar').exists()
        assertApiJar(['org/example/server/api/Component.class'])
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
                implementation project(path: ':providing', configuration:'moduleApi')
            }
        """

        then:
        gradleRunner("assemble").forwardOutput().build()
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
        file(root,'src/main/java/org/example/server/api/Component.java') << """package org.example.server.api;

public class Component {

    public Component() {
    }

}
"""
        file(root, 'src/main/java/org/example/server/impl/SomethingInternal.java') << """package org.example.server.impl;

public class SomethingInternal {
}
"""

    }


    void writeConsumingJavaSource(File root = testProjectDir.root) {
        file(root,'src/main/java/org/consuming/Consuming.java') << """package org.consuming;

import org.example.server.api.Component;

public class Consuming {

    public Consuming() {
        Component c = new Component();
    }

}
"""
        file(root, 'src/main/java/org/example/server/impl/SomethingInternal.java') << """package org.example.server.impl;

public class SomethingInternal {
}
"""

    }
}
