/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.processor.model.NativeType;

import java.lang.classfile.CodeBuilder;
import java.lang.constant.ClassDesc;

/**
 * Shared class descriptors and bytecode helpers used by all class writers in this package.
 */
final class ClassWriterUtil {

    // Standard Java types
    static final ClassDesc CD_void = ClassDesc.ofDescriptor("V");
    static final ClassDesc CD_Object = ClassDesc.of("java.lang.Object");
    static final ClassDesc CD_String = ClassDesc.of("java.lang.String");
    static final ClassDesc CD_long = ClassDesc.ofDescriptor("J");

    // java.lang.foreign types
    static final ClassDesc CD_MemoryLayout = ClassDesc.of("java.lang.foreign.MemoryLayout");
    static final ClassDesc CD_MemoryLayoutArray = ClassDesc.ofDescriptor("[Ljava/lang/foreign/MemoryLayout;");
    static final ClassDesc CD_MemorySegment = ClassDesc.of("java.lang.foreign.MemorySegment");
    private static final ClassDesc CD_ValueLayout = ClassDesc.of("java.lang.foreign.ValueLayout");

    // org.elasticsearch.foreign.adapter helpers
    static final ClassDesc CD_MemorySegmentAdapter = ClassDesc.of("org.elasticsearch.foreign.adapter.MemorySegmentAdapter");

    /**
     * Converts a Java release number (e.g. 21) to the corresponding class file major version
     * (e.g. 65) for use with {@link java.lang.classfile.ClassBuilder#withVersion}.
     */
    static int classFileVersion(int javaRelease) {
        return 44 + javaRelease;
    }

    private ClassWriterUtil() {}

    /** Maps a primitive {@link NativeType} to its JVM {@link ClassDesc}. Throws on non-primitive types. */
    static ClassDesc primitiveClassDesc(NativeType type) {
        return switch (type) {
            case INT -> ClassDesc.ofDescriptor("I");
            case LONG -> ClassDesc.ofDescriptor("J");
            case SHORT -> ClassDesc.ofDescriptor("S");
            case BYTE -> ClassDesc.ofDescriptor("B");
            case BOOLEAN -> ClassDesc.ofDescriptor("Z");
            case FLOAT -> ClassDesc.ofDescriptor("F");
            case DOUBLE -> ClassDesc.ofDescriptor("D");
            case VOID, ADDRESS, STRING -> throw new AssertionError("not a primitive type: " + type);
        };
    }

    /** Pushes the {@code ValueLayout} constant for the given type onto the operand stack. */
    static void emitValueLayout(CodeBuilder cb, NativeType type) {
        record VLField(String name, ClassDesc type) {}
        VLField vl = switch (type) {
            case INT -> new VLField("JAVA_INT", ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfInt;"));
            case LONG -> new VLField("JAVA_LONG", ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfLong;"));
            case SHORT -> new VLField("JAVA_SHORT", ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfShort;"));
            case BYTE -> new VLField("JAVA_BYTE", ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfByte;"));
            case BOOLEAN -> new VLField("JAVA_BOOLEAN", ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfBoolean;"));
            case FLOAT -> new VLField("JAVA_FLOAT", ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfFloat;"));
            case DOUBLE -> new VLField("JAVA_DOUBLE", ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfDouble;"));
            case ADDRESS, STRING -> new VLField("ADDRESS", ClassDesc.ofDescriptor("Ljava/lang/foreign/AddressLayout;"));
            case VOID -> throw new AssertionError("void has no ValueLayout");
        };
        cb.getstatic(CD_ValueLayout, vl.name(), vl.type());
    }

    /** Returns the Java {@link ClassDesc} for a struct field's type. */
    static ClassDesc fieldClassDesc(NativeType type) {
        return switch (type) {
            case ADDRESS, STRING -> CD_MemorySegment;
            case VOID -> throw new AssertionError("void cannot be a struct field type");
            default -> primitiveClassDesc(type);
        };
    }
}
