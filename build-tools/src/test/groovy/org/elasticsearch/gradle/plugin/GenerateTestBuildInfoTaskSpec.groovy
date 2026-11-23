/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.plugin

import org.gradle.testfixtures.ProjectBuilder

import spock.lang.Specification
import spock.lang.TempDir

import java.util.jar.JarEntry
import java.util.jar.JarOutputStream

class GenerateTestBuildInfoTaskSpec extends Specification {

    @TempDir
    File tmp

    def "representative class is the lexicographically smallest regardless of directory content order"() {
        given:
        def classes = new File(tmp, "classes")
        ["z/Last.class", "m/Middle.class", "a/b/First.class", "a/First\$Inner.class"].each {
            def f = new File(classes, it)
            f.parentFile.mkdirs()
            f.text = "x"
        }

        expect:
        generate(classes).contains('"representative_class" : "a/b/First.class"')
    }

    def "representative class is the lexicographically smallest regardless of jar entry order"() {
        given:
        def jar = new File(tmp, "some-lib-1.0.jar")
        new JarOutputStream(new FileOutputStream(jar)).withCloseable { out ->
            ["z/Last.class", "m/Middle.class", "a/First.class", "a/First\$Inner.class", "META-INF/Aaa.class"].each {
                out.putNextEntry(new JarEntry(it))
                out.write(1)
                out.closeEntry()
            }
        }

        expect:
        generate(jar).contains('"representative_class" : "a/First.class"')
    }

    private String generate(File location) {
        def project = ProjectBuilder.builder().withProjectDir(tmp).build()
        def task = project.tasks.register("gen", GenerateTestBuildInfoTask).get()
        task.componentName.set("comp")
        task.moduleName.set("mod")
        task.codeLocations.set(project.files(location))
        def out = new File(tmp, "out-" + location.name + ".json")
        task.outputFile.set(out)
        task.generatePropertiesFile()
        return out.text
    }
}
