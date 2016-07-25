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
import org.objectweb.asm.Opcodes;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a single variable declaration.
 */
public final class SDeclaration extends AStatement {

    private final String type;
    private final String name;
    private AExpression expression;

    private Variable variable = null;

    public SDeclaration(Location location, String type, String name, AExpression expression) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.name = Objects.requireNonNull(name);
        this.expression = expression;
    }

    @Override
    void extractVariables(Set<String> variables) {
        variables.add(name);

        if (expression != null) {
            expression.extractVariables(variables);
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

        if (expression != null) {
            expression.expected = type;
            expression.analyze(locals);
            expression = expression.cast(locals);
        }

        variable = locals.addVariable(location, type, name, false);
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeStatementOffset(location);

        if (expression == null) {
            switch (variable.type.sort) {
                case VOID:   throw createError(new IllegalStateException("Illegal tree structure."));
                case BOOL:
                case BYTE:
                case SHORT:
                case CHAR:
                case INT:    writer.push(0);    break;
                case LONG:   writer.push(0L);   break;
                case FLOAT:  writer.push(0.0F); break;
                case DOUBLE: writer.push(0.0);  break;
                default:     writer.visitInsn(Opcodes.ACONST_NULL);
            }
        } else {
            expression.write(writer, globals);
        }

        writer.visitVarInsn(variable.type.type.getOpcode(Opcodes.ISTORE), variable.getSlot());
    }
}
