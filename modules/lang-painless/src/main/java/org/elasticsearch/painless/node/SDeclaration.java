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

import org.elasticsearch.painless.CompilerSettings;
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

    private Variable variable = null;

    public SDeclaration(Location location, String type, String name, AExpression expression) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.name = Objects.requireNonNull(name);
        children.add(expression);
    }

    @Override
    void storeSettings(CompilerSettings settings) {
        if (children.get(0) != null) {
            children.get(0).storeSettings(settings);
        }
    }

    @Override
    void extractVariables(Set<String> variables) {
        variables.add(name);

        if (children.get(0) != null) {
            children.get(0).extractVariables(variables);
        }
    }

    @Override
    void analyze(Locals locals) {
        Class<?> clazz = locals.getPainlessLookup().canonicalTypeNameToType(this.type);

        if (clazz == null) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        AExpression expression = (AExpression)children.get(0);

        if (expression != null) {
            expression.expected = clazz;
            expression.analyze(locals);
            children.set(0, expression.cast(locals));
        }

        variable = locals.addVariable(location, clazz, name, false);
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeStatementOffset(location);

        if (children.get(0) == null) {
            Class<?> sort = variable.clazz;

            if (sort == void.class || sort == boolean.class || sort == byte.class ||
                sort == short.class || sort == char.class || sort == int.class) {
                writer.push(0);
            } else if (sort == long.class) {
                writer.push(0L);
            } else if (sort == float.class) {
                writer.push(0F);
            } else if (sort == double.class) {
                writer.push(0D);
            } else {
                writer.visitInsn(Opcodes.ACONST_NULL);
            }
        } else {
            children.get(0).write(writer, globals);
        }

        writer.visitVarInsn(MethodWriter.getType(variable.clazz).getOpcode(Opcodes.ISTORE), variable.getSlot());
    }

    @Override
    public String toString() {
        if (children.get(0) == null) {
            return singleLineToString(type, name);
        }
        return singleLineToString(type, name, children.get(0));
    }
}
