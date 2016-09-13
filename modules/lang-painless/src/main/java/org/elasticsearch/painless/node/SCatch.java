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

import org.elasticsearch.painless.Definition;
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
 * Represents a catch block as part of a try-catch block.
 */
public final class SCatch extends AStatement {

    private final String type;
    private final String name;
    private final SBlock block;

    private Variable variable = null;

    Label begin = null;
    Label end = null;
    Label exception = null;

    public SCatch(Location location, String type, String name, SBlock block) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.name = Objects.requireNonNull(name);
        this.block = block;
    }

    @Override
    void extractVariables(Set<String> variables) {
        variables.add(name);

        if (block != null) {
            block.extractVariables(variables);
        }
    }

    @Override
    void analyze(Locals locals) {
        final Type type;

        try {
            type = Definition.getType(this.type);
        } catch (IllegalArgumentException exception) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        if (!Exception.class.isAssignableFrom(type.clazz)) {
            throw createError(new ClassCastException("Not an exception type [" + this.type + "]."));
        }

        variable = locals.addVariable(location, type, name, true);

        if (block != null) {
            block.lastSource = lastSource;
            block.inLoop = inLoop;
            block.lastLoop = lastLoop;

            block.analyze(locals);

            methodEscape = block.methodEscape;
            loopEscape = block.loopEscape;
            allEscape = block.allEscape;
            anyContinue = block.anyContinue;
            anyBreak = block.anyBreak;
            statementCount = block.statementCount;
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeStatementOffset(location);

        Label jump = new Label();

        writer.mark(jump);
        writer.visitVarInsn(variable.type.type.getOpcode(Opcodes.ISTORE), variable.getSlot());

        if (block != null) {
            block.continu = continu;
            block.brake = brake;
            block.write(writer, globals);
        }

        writer.visitTryCatchBlock(begin, end, jump, variable.type.type.getInternalName());

        if (exception != null && !block.allEscape) {
            writer.goTo(exception);
        }
    }
}
