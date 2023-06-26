/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.fixtures

import net.bytebuddy.ByteBuddy
import net.bytebuddy.ClassFileVersion
import net.bytebuddy.description.modifier.Ownership
import net.bytebuddy.description.modifier.Visibility
import net.bytebuddy.description.type.TypeDescription
import net.bytebuddy.dynamic.DynamicType
import net.bytebuddy.implementation.ExceptionMethod
import net.bytebuddy.implementation.FixedValue
import net.bytebuddy.implementation.Implementation

import static org.junit.Assert.fail

class TestClasspathUtils {

    static void setupJarHellJar(File projectRoot) {
        generateJdkJarHellCheck(projectRoot, "org.elasticsearch.jdk.JarHell", FixedValue.value(TypeDescription.VOID))
    }

    static void setupJarJdkClasspath(File projectRoot) {
        generateJdkJarHellCheck(projectRoot, "org.elasticsearch.jdk.JdkJarHellCheck", FixedValue.value(TypeDescription.VOID))
    }

    static void setupJarJdkClasspath(File projectRoot, String errorMessage) {
        generateJdkJarHellCheck(projectRoot, "org.elasticsearch.jdk.JdkJarHellCheck",
                ExceptionMethod.throwing(IllegalStateException.class, errorMessage))
    }

    private static void generateJdkJarHellCheck(File targetDir, String className, Implementation mainImplementation) {
        DynamicType.Unloaded<?> dynamicType = new ByteBuddy(ClassFileVersion.ofJavaVersion(8)).subclass(Object.class)
            .name(className)
            .defineMethod("main", void.class, Visibility.PUBLIC, Ownership.STATIC)
            .withParameters(String[].class)
            .intercept(mainImplementation)
            .make()
        try {
            dynamicType.toJar(targetFile(targetDir))
        } catch (IOException e) {
            e.printStackTrace()
            fail("Cannot setup jdk jar hell classpath")
        }
    }

    private static File targetFile(File projectRoot) {
        File targetFile = new File(projectRoot, "elasticsearch-core-current.jar")
        targetFile.getParentFile().mkdirs()
        return targetFile
    }

    static class JdkJarHellBase {
        static void main(String[] args) {
            System.out.println("args = " + args)
        }
    }
}
