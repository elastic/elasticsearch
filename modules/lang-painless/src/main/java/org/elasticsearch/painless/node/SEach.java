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
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.MethodKey;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_HANDLE;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_HASNEXT;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_NEXT;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_TYPE;

/**
 * Represents a for-each loop shortcut for iterables.  Defers to other S-nodes for non-iterable types.
 */
public class SEach extends AStatement {

    final String type;
    final String name;
    AExpression expression;
    final SBlock block;

    // Members for all cases.
    Variable variable = null;
    Cast cast = null;

    // Members for the array case.
    Variable array = null;
    Variable index = null;
    Type indexed = null;

    // Members for the iterable case.
    Variable iterator = null;
    Method method = null;

    public SEach(Location location, String type, String name, AExpression expression, SBlock block) {
        super(location);

        this.type = type;
        this.name = name;
        this.expression = expression;
        this.block = block;
    }

    @Override
    void analyze(Locals locals) {
        expression.analyze(locals);
        expression.expected = expression.actual;
        expression = expression.cast(locals);

        final Type type;

        try {
            type = Definition.getType(this.type);
        } catch (IllegalArgumentException exception) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        locals.incrementScope();

        variable = locals.addVariable(location, type, name, true, false);

        if (expression.actual.sort == Sort.ARRAY) {
            analyzeArray(locals, type);
        } else if (expression.actual.sort == Sort.DEF || Iterable.class.isAssignableFrom(expression.actual.clazz)) {
            analyzeIterable(locals, type);
        } else {
            throw createError(new IllegalArgumentException("Illegal for each type [" + expression.actual.name + "]."));
        }

        if (block == null) {
            throw createError(new IllegalArgumentException("Extraneous for each loop."));
        }

        block.beginLoop = true;
        block.inLoop = true;
        block.analyze(locals);
        block.statementCount = Math.max(1, block.statementCount);

        if (block.loopEscape && !block.anyContinue) {
            throw createError(new IllegalArgumentException("Extraneous for loop."));
        }

        statementCount = 1;

        if (locals.getMaxLoopCounter() > 0) {
            loopCounterSlot = locals.getVariable(location, "#loop").slot;
        }

        locals.decrementScope();
    }

    void analyzeArray(Locals variables, Type type) {
        // We must store the array and index as variables for securing slots on the stack, and
        // also add the location offset to make the names unique in case of nested for each loops.
        array = variables.addVariable(location, expression.actual, "#array" + location.getOffset(), true, false);
        index = variables.addVariable(location, Definition.INT_TYPE, "#index" + location.getOffset(), true, false);
        indexed = Definition.getType(expression.actual.struct, expression.actual.dimensions - 1);
        cast = AnalyzerCaster.getLegalCast(location, indexed, type, true, true);
    }

    void analyzeIterable(Locals variables, Type type) {
        // We must store the iterator as a variable for securing a slot on the stack, and
        // also add the location offset to make the name unique in case of nested for each loops.
        iterator = variables.addVariable(location, Definition.getType("Iterator"), "#itr" + location.getOffset(), true, false);

        if (expression.actual.sort == Sort.DEF) {
            method = null;
        } else {
            method = expression.actual.struct.methods.get(new MethodKey("iterator", 0));

            if (method == null) {
                throw createError(new IllegalArgumentException(
                    "Unable to create iterator for the type [" + expression.actual.name + "]."));
            }
        }

        cast = AnalyzerCaster.getLegalCast(location, Definition.DEF_TYPE, type, true, true);
    }

    @Override
    void write(MethodWriter writer) {
        writer.writeStatementOffset(location);

        if (array != null) {
            writeArray(writer);
        } else if (iterator != null) {
            writeIterable(writer);
        } else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    void writeArray(MethodWriter writer) {
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

    void writeIterable(MethodWriter writer) {
        expression.write(writer);

        if (method == null) {
            Type itr = Definition.getType("Iterator");
            String desc = org.objectweb.asm.Type.getMethodDescriptor(itr.type, Definition.DEF_TYPE.type);
            writer.invokeDynamic("iterator", desc, DEF_BOOTSTRAP_HANDLE, DefBootstrap.ITERATOR);
        } else if (java.lang.reflect.Modifier.isInterface(method.owner.clazz.getModifiers())) {
            writer.invokeInterface(method.owner.type, method.method);
        } else {
            writer.invokeVirtual(method.owner.type, method.method);
        }

        writer.visitVarInsn(iterator.type.type.getOpcode(Opcodes.ISTORE), iterator.slot);

        Label begin = new Label();
        Label end = new Label();

        writer.mark(begin);

        writer.visitVarInsn(iterator.type.type.getOpcode(Opcodes.ILOAD), iterator.slot);
        writer.invokeInterface(ITERATOR_TYPE, ITERATOR_HASNEXT);
        writer.ifZCmp(MethodWriter.EQ, end);

        writer.visitVarInsn(iterator.type.type.getOpcode(Opcodes.ILOAD), iterator.slot);
        writer.invokeInterface(ITERATOR_TYPE, ITERATOR_NEXT);
        writer.writeCast(cast);
        writer.visitVarInsn(variable.type.type.getOpcode(Opcodes.ISTORE), variable.slot);

        block.write(writer);

        writer.goTo(begin);
        writer.mark(end);
    }
}
