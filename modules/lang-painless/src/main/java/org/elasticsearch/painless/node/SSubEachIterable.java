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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.painless.WriterConstants.ITERATOR_HASNEXT;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_NEXT;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_TYPE;

/**
 * Represents a for-each loop for iterables.
 */
final class SSubEachIterable extends AStatement {

    private AExpression expression;
    private final SBlock block;
    private final Variable variable;

    private Cast cast = null;
    private Variable iterator = null;
    private Method method = null;

    SSubEachIterable(Location location, Variable variable, AExpression expression, SBlock block) {
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
        // We must store the iterator as a variable for securing a slot on the stack, and
        // also add the location offset to make the name unique in case of nested for each loops.
        iterator = locals.addVariable(location, locals.getDefinition().getType("Iterator"),
                "#itr" + location.getOffset(), true);

        if (expression.actual.dynamic) {
            method = null;
        } else {
            method = expression.actual.struct.methods.get(new MethodKey("iterator", 0));

            if (method == null) {
                throw createError(new IllegalArgumentException(
                    "Unable to create iterator for the type [" + expression.actual.name + "]."));
            }
        }

        cast = locals.getDefinition().caster.getLegalCast(location, locals.getDefinition().DefType, variable.type, true, true);
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeStatementOffset(location);

        expression.write(writer, globals);

        if (method == null) {
            org.objectweb.asm.Type methodType = org.objectweb.asm.Type
                    .getMethodType(org.objectweb.asm.Type.getType(Iterator.class), org.objectweb.asm.Type.getType(Object.class));
            writer.invokeDefCall("iterator", methodType, DefBootstrap.ITERATOR);
        } else {
            method.write(writer);
        }

        writer.visitVarInsn(iterator.type.type.getOpcode(Opcodes.ISTORE), iterator.getSlot());

        Label begin = new Label();
        Label end = new Label();

        writer.mark(begin);

        writer.visitVarInsn(iterator.type.type.getOpcode(Opcodes.ILOAD), iterator.getSlot());
        writer.invokeInterface(ITERATOR_TYPE, ITERATOR_HASNEXT);
        writer.ifZCmp(MethodWriter.EQ, end);

        writer.visitVarInsn(iterator.type.type.getOpcode(Opcodes.ILOAD), iterator.getSlot());
        writer.invokeInterface(ITERATOR_TYPE, ITERATOR_NEXT);
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
