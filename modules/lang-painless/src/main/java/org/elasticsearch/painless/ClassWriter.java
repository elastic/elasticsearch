/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.util.Printer;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.Closeable;
import java.lang.reflect.Modifier;
import java.util.BitSet;

/**
 * Manages the top level writers for class and possibly
 * clinit if necessary.
 */
public class ClassWriter implements Closeable  {

    /**
     * Converts Java reflection modifiers to ASM access constants.
     * @param modifiers Java reflection {@code Modifiers}
     * @param synthetic {@code true} if the item is synthetically generated
     * @return ASM access constants
     */
    public static int buildAccess(int modifiers, boolean synthetic) {
        int access = synthetic ? Opcodes.ACC_SYNTHETIC : 0;

        if (Modifier.isFinal(modifiers))        access |= Opcodes.ACC_FINAL;
        if (Modifier.isInterface(modifiers))    access |= Opcodes.ACC_INTERFACE;
        if (Modifier.isNative(modifiers))       access |= Opcodes.ACC_NATIVE;
        if (Modifier.isPrivate(modifiers))      access |= Opcodes.ACC_PRIVATE;
        if (Modifier.isProtected(modifiers))    access |= Opcodes.ACC_PROTECTED;
        if (Modifier.isPublic(modifiers))       access |= Opcodes.ACC_PUBLIC;
        if (Modifier.isStatic(modifiers))       access |= Opcodes.ACC_STATIC;
        if (Modifier.isStrict(modifiers))       access |= Opcodes.ACC_STRICT;
        if (Modifier.isSynchronized(modifiers)) access |= Opcodes.ACC_SYNCHRONIZED;
        if (Modifier.isTransient(modifiers))    access |= Opcodes.ACC_TRANSIENT;
        if (Modifier.isVolatile(modifiers))     access |= Opcodes.ACC_VOLATILE;

        return access;
    }

    protected final CompilerSettings compilerSettings;
    protected final BitSet statements;

    protected final org.objectweb.asm.ClassWriter classWriter;
    protected final ClassVisitor classVisitor;
    protected MethodWriter clinitWriter = null;

    public ClassWriter(CompilerSettings compilerSettings, BitSet statements, Printer debugStream,
            Class<?> baseClass, int classFrames, int classAccess, String className, String[] classInterfaces) {

        this.compilerSettings = compilerSettings;
        this.statements = statements;

        classWriter = new org.objectweb.asm.ClassWriter(classFrames);
        ClassVisitor visitor = classWriter;

        if (compilerSettings.isPicky()) {
            visitor = new SimpleChecksAdapter(visitor);
        }

        if (debugStream != null) {
            visitor = new TraceClassVisitor(visitor, debugStream, null);
        }

        classVisitor = visitor;
        classVisitor.visit(WriterConstants.CLASS_VERSION, classAccess, className, null,
                Type.getType(baseClass).getInternalName(), classInterfaces);
    }

    public ClassVisitor getClassVisitor() {
        return classVisitor;
    }

    /**
     * Lazy loads the {@link MethodWriter} for clinit, so that if it's not
     * necessary the method is never created for the class.
     */
    public MethodWriter getClinitWriter() {
        if (clinitWriter == null) {
            clinitWriter = new MethodWriter(Opcodes.ACC_STATIC, WriterConstants.CLINIT, classVisitor, statements, compilerSettings);
            clinitWriter.visitCode();
        }

        return clinitWriter;
    }

    public MethodWriter newMethodWriter(int access, Method method) {
        return new MethodWriter(access, method, classVisitor, statements, compilerSettings);
    }

    @Override
    public void close() {
        if (clinitWriter != null) {
            clinitWriter.returnValue();
            clinitWriter.endMethod();
        }

        classVisitor.visitEnd();
    }

    public byte[] getClassBytes() {
        return classWriter.toByteArray();
    }
}
