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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Executable;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.ExecuteReserved;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.WriterConstants;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.painless.WriterConstants.BASE_CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.CONSTRUCTOR;
import static org.elasticsearch.painless.WriterConstants.EXECUTE;
import static org.elasticsearch.painless.WriterConstants.MAP_GET;
import static org.elasticsearch.painless.WriterConstants.MAP_TYPE;

/**
 * The root of all Painless trees.  Contains a series of statements.
 */
public final class SSource extends AStatement {

    final String name;
    final String source;
    final ExecuteReserved reserved;
    final List<AStatement> statements;

    private Locals locals;

    public SSource(String name, String source, ExecuteReserved reserved, Location location, List<AStatement> statements) {
        super(location);

        this.name = name;
        this.source = source;
        this.reserved = reserved;
        this.statements = Collections.unmodifiableList(statements);

        locals = new Locals(reserved, null);
    }

    public void analyze() {
        analyze(locals);
    }

    @Override
    void analyze(Locals locals) {
        if (statements == null || statements.isEmpty()) {
            throw createError(new IllegalArgumentException("Cannot generate an empty script."));
        }

        locals.incrementScope();

        AStatement last = statements.get(statements.size() - 1);

        for (AStatement statement : statements) {
            // Note that we do not need to check after the last statement because
            // there is no statement that can be unreachable after the last.
            if (allEscape) {
                throw createError(new IllegalArgumentException("Unreachable statement."));
            }

            statement.lastSource = statement == last;

            statement.analyze(locals);

            methodEscape = statement.methodEscape;
            allEscape = statement.allEscape;
        }

        locals.decrementScope();
    }

    byte[] write() {
        // Create the ClassWriter.

        int classFrames = ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS;
        int classVersion = Opcodes.V1_8;
        int classAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        String classBase = BASE_CLASS_TYPE.getInternalName();
        String className = CLASS_TYPE.getInternalName();
        String classInterfaces[] = reserved.usesScore() ? new String[] { WriterConstants.NEEDS_SCORE_TYPE.getInternalName() } : null;

        ClassWriter writer = new ClassWriter(classFrames);
        writer.visit(classVersion, classAccess, className, null, classBase, classInterfaces);
        writer.visitSource(Location.computeSourceName(name, source), null);

        // Create the main MethodWriter.

        BitSet expressions = new BitSet(source.length());
        MethodWriter execute = new MethodWriter(Opcodes.ACC_PUBLIC, EXECUTE, writer, expressions);

        // Write the constructor.

        MethodWriter constructor = execute.newMethodWriter(Opcodes.ACC_PUBLIC, CONSTRUCTOR);
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(org.objectweb.asm.Type.getType(Executable.class), CONSTRUCTOR);
        constructor.returnValue();
        constructor.endMethod();

        // Write the execute method.

        if (reserved.usesScore()) {
            // if the _score value is used, we do this once:
            // final double _score = scorer.score();
            Variable scorer = locals.getVariable(null, ExecuteReserved.SCORER);
            Variable score = locals.getVariable(null, ExecuteReserved.SCORE);

            execute.visitVarInsn(Opcodes.ALOAD, scorer.slot);
            execute.invokeVirtual(WriterConstants.SCORER_TYPE, WriterConstants.SCORER_SCORE);
            execute.visitInsn(Opcodes.F2D);
            execute.visitVarInsn(Opcodes.DSTORE, score.slot);
        }

        if (reserved.usesCtx()) {
            // if the _ctx value is used, we do this once:
            // final Map<String,Object> ctx = input.get("ctx");

            Variable input = locals.getVariable(null, ExecuteReserved.PARAMS);
            Variable ctx = locals.getVariable(null, ExecuteReserved.CTX);

            execute.visitVarInsn(Opcodes.ALOAD, input.slot);
            execute.push(ExecuteReserved.CTX);
            execute.invokeInterface(MAP_TYPE, MAP_GET);
            execute.visitVarInsn(Opcodes.ASTORE, ctx.slot);
        }

        if (reserved.getMaxLoopCounter() > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()

            Variable loop = locals.getVariable(null, ExecuteReserved.LOOP);

            execute.push(reserved.getMaxLoopCounter());
            execute.visitVarInsn(Opcodes.ISTORE, loop.slot);
        }

        write(execute);
        execute.endMethod();

        // End writing the class and return the generated bytes.

        writer.visitEnd();

        return writer.toByteArray();
    }

    @Override
    void write(MethodWriter writer) {
        for (AStatement statement : statements) {
            statement.write(writer);
        }

        if (!methodEscape) {
            writer.visitInsn(Opcodes.ACONST_NULL);
            writer.returnValue();
        }
    }
}
