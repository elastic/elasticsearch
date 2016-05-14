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

/**
 * Runs the writing phase of compilation using the Painless AST.
 */
final class Writer {

    static byte[] write(final CompilerSettings settings, final Definition definition,
                               String name, final String source, final Variables variables, final SSource root) {
        final Writer writer = new Writer(settings, definition, name, source, variables, root);

        return writer.getBytes();
    }

    private final CompilerSettings settings;
    private final Definition definition;
    private final String scriptName;
    private final String source;
    private final Variables variables;
    private final SSource root;

    private final ClassWriter writer;
    private final GeneratorAdapter adapter;

    private Writer(final CompilerSettings settings, final Definition definition,
                     String name, final String source, final Variables variables, final SSource root) {
        this.settings = settings;
        this.definition = definition;
        this.scriptName = name;
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

    // This maximum length is theoretically 65535 bytes, but as it's CESU-8 encoded we dont know how large it is in bytes, so be safe
    private static final int MAX_NAME_LENGTH = 256;

    private void writeBegin() {
        final int version = Opcodes.V1_8;
        final int access = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        final String base = BASE_CLASS_TYPE.getInternalName();
        final String name = CLASS_TYPE.getInternalName();

        // apply marker interface NeedsScore if we use the score!
        final String interfaces[] = variables.reserved.score ?
            new String[] { WriterConstants.NEEDS_SCORE_TYPE.getInternalName() } : null;

        writer.visit(version, access, name, null, base, interfaces);
        writer.visitSource(computeSourceName(), null);
    }

    /** Computes the file name (mostly important for stacktraces) */
    private String computeSourceName() {
        StringBuilder fileName = new StringBuilder();
        if (scriptName.equals(PainlessScriptEngineService.INLINE_NAME)) {
            // its an anonymous script, include at least a portion of the source to help identify which one it is
            // but don't create stacktraces with filenames that contain newlines or huge names.

            // truncate to the first newline
            int limit = source.indexOf('\n');
            if (limit >= 0) {
                int limit2 = source.indexOf('\r');
                if (limit2 >= 0) {
                    limit = Math.min(limit, limit2);
                }
            } else {
                limit = source.length();
            }

            // truncate to our limit
            limit = Math.min(limit, MAX_NAME_LENGTH);
            fileName.append(source, 0, limit);

            // if we truncated, make it obvious
            if (limit != source.length()) {
                fileName.append(" ...");
            }            
            fileName.append(" @ <inline script>");
        } else {
            // its a named script, just use the name
            // but don't trust this has a reasonable length!
            if (scriptName.length() > MAX_NAME_LENGTH) {
                fileName.append(scriptName, 0, MAX_NAME_LENGTH);
                fileName.append(" ...");
            } else {
                fileName.append(scriptName);
            }
        }
        return fileName.toString();
    }

    private void writeConstructor() {
        final GeneratorAdapter constructor = new GeneratorAdapter(Opcodes.ACC_PUBLIC, CONSTRUCTOR, null, null, writer);
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(org.objectweb.asm.Type.getType(Executable.class), CONSTRUCTOR);
        constructor.returnValue();
        constructor.endMethod();
    }

    private void writeExecute() {
        if (variables.reserved.score) {
            // if the _score value is used, we do this once:
            // final double _score = scorer.score();
            final Variable scorer = variables.getVariable(null, Reserved.SCORER);
            final Variable score = variables.getVariable(null, Reserved.SCORE);

            adapter.visitVarInsn(Opcodes.ALOAD, scorer.slot);
            adapter.invokeVirtual(WriterConstants.SCORER_TYPE, WriterConstants.SCORER_SCORE);
            adapter.visitInsn(Opcodes.F2D);
            adapter.visitVarInsn(Opcodes.DSTORE, score.slot);
        }

        if (variables.reserved.ctx) {
            // if the _ctx value is used, we do this once:
            // final Map<String,Object> ctx = input.get("ctx");

            final Variable input = variables.getVariable(null, Reserved.PARAMS);
            final Variable ctx = variables.getVariable(null, Reserved.CTX);

            adapter.visitVarInsn(Opcodes.ALOAD, input.slot);
            adapter.push(Reserved.CTX);
            adapter.invokeInterface(MAP_TYPE, MAP_GET);
            adapter.visitVarInsn(Opcodes.ASTORE, ctx.slot);
        }

        if (variables.reserved.loop) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()

            final Variable loop = variables.getVariable(null, Reserved.LOOP);

            adapter.push(settings.getMaxLoopCounter());
            adapter.visitVarInsn(Opcodes.ISTORE, loop.slot);
        }

        root.write(settings, definition, adapter);
        adapter.endMethod();
    }

    private void writeEnd() {
        writer.visitEnd();
    }

    private byte[] getBytes() {
        return writer.toByteArray();
    }
}
