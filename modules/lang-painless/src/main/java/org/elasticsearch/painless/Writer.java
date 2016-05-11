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

import org.elasticsearch.painless.Variables.Reserved;
import org.elasticsearch.painless.Variables.Variable;
import org.elasticsearch.painless.node.SSource;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;

import static org.elasticsearch.painless.WriterConstants.BASE_CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.CONSTRUCTOR;
import static org.elasticsearch.painless.WriterConstants.EXECUTE;
import static org.elasticsearch.painless.WriterConstants.MAP_GET;
import static org.elasticsearch.painless.WriterConstants.MAP_TYPE;

public class Writer {
    public static byte[] write(final CompilerSettings settings, final Definition definition,
                               final String source, final Variables variables, final SSource root) {
        final Writer writer = new Writer(settings, definition, source, variables, root);

        return writer.getBytes();
    }

    protected final CompilerSettings settings;
    protected final Definition definition;
    protected final String source;
    protected final Variables variables;
    protected final SSource root;

    protected final ClassWriter writer;
    protected final GeneratorAdapter adapter;

    protected Writer(final CompilerSettings settings, final Definition definition,
                     final String source, final Variables variables, final SSource root) {
        this.settings = settings;
        this.definition = definition;
        this.source = source;
        this.variables = variables;
        this.root = root;

        writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);

        writeBegin();
        writeConstructor();

        adapter = new GeneratorAdapter(Opcodes.ACC_PUBLIC, EXECUTE, null, null, writer);

        writeExecute();
        writeEnd();
    }

    protected void writeBegin() {
        final int version = Opcodes.V1_7;
        final int access = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        final String base = BASE_CLASS_TYPE.getInternalName();
        final String name = CLASS_TYPE.getInternalName();

        // apply marker interface NeedsScore if we use the score!
        final String interfaces[] = variables.reserved.score ?
            new String[] { WriterConstants.NEEDS_SCORE_TYPE.getInternalName() } : null;

        writer.visit(version, access, name, null, base, interfaces);
        writer.visitSource(source, null);
    }

    protected void writeConstructor() {
        final GeneratorAdapter constructor = new GeneratorAdapter(Opcodes.ACC_PUBLIC, CONSTRUCTOR, null, null, writer);
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(org.objectweb.asm.Type.getType(Executable.class), CONSTRUCTOR);
        constructor.returnValue();
        constructor.endMethod();
    }

    protected void writeExecute() {
        if (variables.reserved.score) {
            final Variable score = variables.getVariable(null, Reserved.SCORE);

            adapter.visitVarInsn(Opcodes.ALOAD, score.slot);
            adapter.invokeVirtual(WriterConstants.SCORER_TYPE, WriterConstants.SCORER_SCORE);
            adapter.visitVarInsn(Opcodes.FSTORE, score.slot);
        }

        if (variables.reserved.ctx) {
            final Variable input = variables.getVariable(null, Reserved.INPUT);
            final Variable ctx = variables.getVariable(null, Reserved.CTX);

            adapter.visitVarInsn(Opcodes.ALOAD, input.slot);
            adapter.push(Reserved.CTX);
            adapter.invokeInterface(MAP_TYPE, MAP_GET);
            adapter.visitVarInsn(Opcodes.ASTORE, ctx.slot);
        }

        if (variables.reserved.loop) {
            final Variable loop = variables.getVariable(null, Reserved.LOOP);

            adapter.push(settings.getMaxLoopCounter());
            adapter.visitVarInsn(Opcodes.ISTORE, loop.slot);
        }

        root.write(settings, definition, adapter);
        adapter.endMethod();
    }

    protected void writeEnd() {
        writer.visitEnd();
    }

    protected byte[] getBytes() {
        return writer.toByteArray();
    }
}
