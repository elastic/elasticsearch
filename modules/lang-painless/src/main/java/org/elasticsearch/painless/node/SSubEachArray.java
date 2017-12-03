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

import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a for-each loop for arrays.
 */
final class SSubEachArray extends AStatement {
    private final Variable variable;
    private AExpression expression;
    private final SBlock block;

    private Cast cast = null;
    private Variable array = null;
    private Variable index = null;
    private Type indexed = null;

    SSubEachArray(Location location, Variable variable, AExpression expression, SBlock block) {
        super(location);

        this.variable = Objects.requireNonNull(variable);
        this.expression = Objects.requireNonNull(expression);
        this.block = block;
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void analyze(Locals locals) {
        // We must store the array and index as variables for securing slots on the stack, and
        // also add the location offset to make the names unique in case of nested for each loops.
        array = locals.addVariable(location, expression.actual, "#array" + location.getOffset(),
                true);
        index = locals.addVariable(location, locals.getDefinition().intType, "#index" + location.getOffset(),
                true);
        indexed = locals.getDefinition().getType(expression.actual.struct,
                expression.actual.dimensions - 1);
        cast = locals.getDefinition().caster.getLegalCast(location, indexed, variable.type, true, true);
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeStatementOffset(location);

        expression.write(writer, globals);
        writer.visitVarInsn(array.type.type.getOpcode(Opcodes.ISTORE), array.getSlot());
        writer.push(-1);
        writer.visitVarInsn(index.type.type.getOpcode(Opcodes.ISTORE), index.getSlot());

        Label begin = new Label();
        Label end = new Label();

        writer.mark(begin);

        writer.visitIincInsn(index.getSlot(), 1);
        writer.visitVarInsn(index.type.type.getOpcode(Opcodes.ILOAD), index.getSlot());
        writer.visitVarInsn(array.type.type.getOpcode(Opcodes.ILOAD), array.getSlot());
        writer.arrayLength();
        writer.ifICmp(MethodWriter.GE, end);

        writer.visitVarInsn(array.type.type.getOpcode(Opcodes.ILOAD), array.getSlot());
        writer.visitVarInsn(index.type.type.getOpcode(Opcodes.ILOAD), index.getSlot());
        writer.arrayLoad(indexed.type);
        writer.writeCast(cast);
        writer.visitVarInsn(variable.type.type.getOpcode(Opcodes.ISTORE), variable.getSlot());

        if (loopCounter != null) {
            writer.writeLoopCounter(loopCounter.getSlot(), statementCount, location);
        }

        block.continu = begin;
        block.brake = end;
        block.write(writer, globals);

        writer.goTo(begin);
        writer.mark(end);
    }

    @Override
    public String toString() {
        return singleLineToString(variable.type.name, variable.name, expression, block);
    }
}
