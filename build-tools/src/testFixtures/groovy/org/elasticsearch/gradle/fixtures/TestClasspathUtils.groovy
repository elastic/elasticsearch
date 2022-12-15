/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.fixtures

import net.bytebuddy.ByteBuddy
import net.bytebuddy.description.modifier.Ownership
import net.bytebuddy.description.modifier.Visibility
import net.bytebuddy.description.type.TypeDescription
import net.bytebuddy.dynamic.DynamicType
import net.bytebuddy.implementation.ExceptionMethod
import net.bytebuddy.implementation.FixedValue
import net.bytebuddy.implementation.Implementation
import net.bytebuddy.implementation.MethodDelegation

import org.elasticsearch.gradle.FakeNamedComponent

import static org.junit.Assert.fail

class TestClasspathUtils {

    static void setupNamedComponentScanner(File projectRoot, String version) {
        def value = MethodDelegation.to(FakeNamedComponent.class)
        generateJarWithClass(projectRoot, "org.elasticsearch.plugin.scanner.NamedComponentScanner","elasticsearch-plugin-scanner",
            version, value
        )
    }

    static void setupJarHellJar(File projectRoot) {
        generateJarWithClass(projectRoot, "org.elasticsearch.jdk.JarHell","elasticsearch-core", "current", FixedValue.value(TypeDescription.VOID))
    }

    static void setupJarHellJar(File projectRoot, String version) {
        generateJarWithClass(projectRoot, "org.elasticsearch.jdk.JarHell", "elasticsearch-core", version, FixedValue.value(TypeDescription.VOID))
    }

    static void setupJarJdkClasspath(File projectRoot) {
        generateJdkJarHellCheck(projectRoot, "org.elasticsearch.jdk.JdkJarHellCheck", FixedValue.value(TypeDescription.VOID))
    }

    static void setupJarJdkClasspath(File projectRoot, String errorMessage) {
        generateJdkJarHellCheck(projectRoot, "org.elasticsearch.jdk.JdkJarHellCheck",
                ExceptionMethod.throwing(IllegalStateException.class, errorMessage))
    }

    private static void generateJdkJarHellCheck(File targetDir, String className, Implementation mainImplementation) {
        generateJarWithClass(targetDir, className, "elasticsearch-core", "current", mainImplementation)
    }


    private static void generateJarWithClass(File targetDir, String className, String artifactName, String version, Implementation mainImplementation) {
        DynamicType.Unloaded<?> dynamicType = new ByteBuddy().subclass(Object.class)
            .name(className)
            .defineMethod("main", void.class, Visibility.PUBLIC, Ownership.STATIC)
            .withParameters(String[].class)
            .intercept(mainImplementation)
            .make()
        try {
            dynamicType.toJar(targetFile(targetDir, artifactName, version))
        } catch (IOException e) {
            e.printStackTrace()
            fail("Cannot setup jdk jar hell classpath")
        }
    }

    private static File targetFile(File projectRoot, String artifactName, String version) {
        File targetFile = new File(projectRoot, "${artifactName}-${version}.jar")

        println "targetFile = $targetFile"
        targetFile.getParentFile().mkdirs()
        return targetFile
    }

    static class JdkJarHellBase {
        static void main(String[] args) {
            System.out.println("args = " + args)
        }
    }
}
