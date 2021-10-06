/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import java.io.File;
import java.io.IOException;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Ownership;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.ExceptionMethod;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;

import static org.junit.Assert.fail;

public class TestClasspathUtils {

    public static void setupJarJdkClasspath(File projectRoot) {
        generateJdkJarHellCheck(projectRoot, FixedValue.value(TypeDescription.VOID));
    }

    public static void setupJarJdkClasspath(File projectRoot, String errorMessage) {
        generateJdkJarHellCheck(projectRoot,
                ExceptionMethod.throwing(IllegalStateException.class, errorMessage));
    }

    private static void generateJdkJarHellCheck(File projectRoot, Implementation mainImplementation) {
        DynamicType.Unloaded<?> dynamicType = new ByteBuddy()
                    .subclass(Object.class)
                    .name("org.elasticsearch.jdk.JdkJarHellCheck")
                .defineMethod("main",   void.class, Visibility.PUBLIC, Ownership.STATIC)
                .withParameters(String[].class)
                .intercept(mainImplementation)
                .make();
        try {
            dynamicType.toJar(targetFile(projectRoot));
        } catch (IOException e) {
            e.printStackTrace();
            fail("Cannot setup jdk jar hell classpath");
        }
    }

    private static File targetFile(File projectRoot) {
        File targetFile = new File(
                projectRoot,
                "sample_jars/build/testrepo/org/elasticsearch/elasticsearch-core/current/elasticsearch-core-current.jar"
        );

        targetFile.getParentFile().mkdirs();
        return targetFile;
    }


    private static class InconsistentParameterReferenceMethod implements net.bytebuddy.implementation.Implementation {
        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return null;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return null;
        }
    }

    public static class JdkJarHellBase {
        public static void main(String[] args) {
            System.out.println("args = " + args);
        }
    }
}
