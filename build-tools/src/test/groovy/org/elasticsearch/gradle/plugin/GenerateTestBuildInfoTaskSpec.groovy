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
import org.objectweb.asm.ClassWriter
import org.objectweb.asm.Opcodes

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream

class GenerateTestBuildInfoTaskSpec extends Specification {

    @TempDir
    File tmp

    /**
     * Drives the directory visitor with an explicit, non-lexicographic visit order: the first eligible class
     * visited is {@code z/Last.class}, so a first-match implementation would pick it. The excluded descriptors
     * and inner class all sort before the expected winner, so removing any of those filters fails the test.
     */
    def "directory representative class is the lexicographically smallest for a known non-lexicographic visit order"() {
        given:
        def classes = new File(tmp, "classes")
        def visitOrder = ["z/Last.class", "a/module-info.class", "n/b/First.class", "o/Middle.class", "module-info.class",
                          "a/First\$Inner.class", "a/Readme.txt"]
        def visitor = new GenerateTestBuildInfoTask.RepresentativeClassVisitor(classes)

        when:
        visitOrder.each {
            def path = writeFile(classes, it).toPath()
            visitor.visitFile(path, Files.readAttributes(path, BasicFileAttributes))
        }

        then:
        visitor.result() == "n/b/First.class"
    }

    def "directory representative class is the lexicographically smallest when walking the file tree"() {
        given:
        def classes = new File(tmp, "classes")
        // The walk order is filesystem dependent, so this only complements the explicit-order test above:
        // the expected winner is created last so that filesystems returning entries in insertion order visit it last.
        def names = (1..50).collect { "pkg${it}/Cls${it}.class".toString() }
        names += ["z/Last.class", "o/Middle.class", "a/First\$Inner.class", "a/module-info.class", "module-info.class",
                  "n/b/First.class"]
        names.each { writeFile(classes, it) }

        expect:
        generate(classes).contains('"representative_class" : "n/b/First.class"')
    }

    def "representative class is the lexicographically smallest regardless of jar entry order"() {
        given:
        def jar = new File(tmp, "some-lib-1.0.jar")
        new JarOutputStream(new FileOutputStream(jar)).withCloseable { out ->
            // module-info.class, META-INF and inner class entries all sort before the expected winner,
            // so removing any of those filters fails the test
            ["z/Last.class", "o/Middle.class", "n/First.class", "a/First\$Inner.class", "META-INF/Aaa.class",
             "module-info.class"].each {
                out.putNextEntry(new JarEntry(it))
                out.write(it.endsWith("module-info.class") ? moduleInfoBytes() : [1] as byte[])
                out.closeEntry()
            }
        }

        expect:
        generate(jar).contains('"representative_class" : "n/First.class"')
    }

    private static File writeFile(File root, String name) {
        def f = new File(root, name)
        f.parentFile.mkdirs()
        // module-info.class files are parsed to find the module name, so they must be valid
        f.bytes = name.endsWith("module-info.class") ? moduleInfoBytes() : "x".bytes
        return f
    }

    private static byte[] moduleInfoBytes() {
        def writer = new ClassWriter(0)
        writer.visit(Opcodes.V17, Opcodes.ACC_MODULE, "module-info", null, null, null)
        writer.visitModule("test.module", 0, null).visitEnd()
        writer.visitEnd()
        return writer.toByteArray()
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
