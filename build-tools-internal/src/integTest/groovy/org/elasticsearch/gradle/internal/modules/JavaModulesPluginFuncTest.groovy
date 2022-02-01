/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.modules

import org.apache.logging.log4j.core.util.Assert
import org.junit.jupiter.api.Assertions

import java.util.zip.ZipEntry
import java.util.zip.ZipFile

class JavaModulesPluginFuncTest extends AbstractJavaModulesPluginFuncTest {

    def "can read module export"() {
        when:
        writeProducingJavaSource(testProjectDir.root, 'root')
        writeModuleInfo(testProjectDir.root,'root')
        buildFile.text = """
            plugins {
             id 'elasticsearch.java'
             id 'elasticsearch.java-modules'
            }
        """

        then:
        gradleRunner("modulesApiJar").build()
        file('build/distributions/hello-world-api.jar').exists()
        assertApiJar(['org/example/root/api/Component.class'])
    }

    private boolean assertApiJar(List<String> expectedEntries) {
        ZipFile jar = new ZipFile(file('build/distributions/hello-world-api.jar'));
        Enumeration<ZipEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
            ZipEntry e = entries.nextElement();
            if (e.getName().endsWith(".class")) {
                println "e.getName() = ${e.getName()}"
                Assertions.assertTrue(expectedEntries.remove(e.getName()))
            }
        }
        Assert.isEmpty(expectedEntries)
    }

}
