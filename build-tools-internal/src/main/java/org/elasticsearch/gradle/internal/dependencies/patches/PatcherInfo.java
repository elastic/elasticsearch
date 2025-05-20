/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.patches;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.function.Function;

public final class PatcherInfo {
    private final String jarEntryName;
    private final byte[] classSha256;
    private final Function<ClassWriter, ClassVisitor> visitorFactory;

    private PatcherInfo(String jarEntryName, byte[] classSha256, Function<ClassWriter, ClassVisitor> visitorFactory) {
        this.jarEntryName = jarEntryName;
        this.classSha256 = classSha256;
        this.visitorFactory = visitorFactory;
    }

    /**
     * Creates a patcher info entry, linking a jar entry path name and its SHA256 digest to a patcher factory (a factory to create an ASM
     * visitor)
     *
     * @param jarEntryName   the jar entry path, as a string
     * @param classSha256    the SHA256 digest of the class bytes, as a HEX string
     * @param visitorFactory the factory to create an ASM visitor from a ASM writer
     */
    public static PatcherInfo classPatcher(String jarEntryName, String classSha256, Function<ClassWriter, ClassVisitor> visitorFactory) {
        return new PatcherInfo(jarEntryName, HexFormat.of().parseHex(classSha256), visitorFactory);
    }

    boolean matches(byte[] otherClassSha256) {
        return Arrays.equals(this.classSha256, otherClassSha256);
    }

    public String jarEntryName() {
        return jarEntryName;
    }

    public byte[] classSha256() {
        return classSha256;
    }

    public ClassVisitor createVisitor(ClassWriter classWriter) {
        return visitorFactory.apply(classWriter);
    }
}
