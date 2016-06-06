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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.Variables.Variable;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

class SArrayEach extends AStatement {
    final int maxLoopCounter;
    final String type;
    final String name;
    AExpression expression;
    AStatement block;

    Variable variable = null;
    Variable array = null;
    Variable index = null;
    Type indexed = null;
    Cast cast = null;

    SArrayEach(final Location location, final int maxLoopCounter,
               final String type, final String name, final AExpression expression, final SBlock block) {
        super(location);

        this.maxLoopCounter = maxLoopCounter;
        this.type = type;
        this.name = name;
        this.expression = expression;
        this.block = block;
    }

    @Override
    AStatement analyze(Variables variables) {
        final Type type;

        try {
            type = Definition.getType(this.type);
        } catch (IllegalArgumentException exception) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        variables.incrementScope();

        variable = variables.addVariable(location, type, name, true, false);
        array = variables.addVariable(location, expression.actual, "#array" + location.getOffset(), true, false);
        index = variables.addVariable(location, Definition.INT_TYPE, "#index" + location.getOffset(), true, false);
        indexed = Definition.getType(expression.actual.struct, expression.actual.dimensions - 1);
        cast = AnalyzerCaster.getLegalCast(location, indexed, type, true, true);

        if (block == null) {
            throw location.createError(new IllegalArgumentException("Extraneous for each loop."));
        }

        block.beginLoop = true;
        block.inLoop = true;
        block = block.analyze(variables);
        block.statementCount = Math.max(1, block.statementCount);

        if (block.loopEscape && !block.anyContinue) {
            throw createError(new IllegalArgumentException("Extraneous for loop."));
        }

        statementCount = 1;

        if (maxLoopCounter > 0) {
            loopCounterSlot = variables.getVariable(location, "#loop").slot;
        }

        variables.decrementScope();

        return this;
    }

    @Override
    void write(MethodWriter writer) {
        writer.writeStatementOffset(location);

        expression.write(writer);
        writer.visitVarInsn(array.type.type.getOpcode(Opcodes.ISTORE), array.slot);
        writer.push(-1);
        writer.visitVarInsn(index.type.type.getOpcode(Opcodes.ISTORE), index.slot);

        Label begin = new Label();
        Label end = new Label();

        writer.mark(begin);

        writer.visitIincInsn(index.slot, 1);
        writer.visitVarInsn(index.type.type.getOpcode(Opcodes.ILOAD), index.slot);
        writer.visitVarInsn(array.type.type.getOpcode(Opcodes.ILOAD), array.slot);
        writer.arrayLength();
        writer.ifICmp(MethodWriter.GE, end);

        writer.visitVarInsn(array.type.type.getOpcode(Opcodes.ILOAD), array.slot);
        writer.visitVarInsn(index.type.type.getOpcode(Opcodes.ILOAD), index.slot);
        writer.arrayLoad(indexed.type);
        writer.writeCast(cast);
        writer.visitVarInsn(variable.type.type.getOpcode(Opcodes.ISTORE), variable.slot);

        block.write(writer);

        writer.goTo(begin);
        writer.mark(end);
    }
}
