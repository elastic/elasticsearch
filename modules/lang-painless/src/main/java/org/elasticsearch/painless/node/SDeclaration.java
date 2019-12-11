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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;
import org.objectweb.asm.Opcodes;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a single variable declaration.
 */
public final class SDeclaration extends AStatement {

    private final DType type;
    private final String name;
    private AExpression expression;

    Variable variable = null;

    public SDeclaration(Location location, DType type, String name, AExpression expression) {
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
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        DResolvedType resolvedType = type.resolveType(scriptRoot.getPainlessLookup());

        if (expression != null) {
            expression.expected = resolvedType.getType();
            expression.analyze(scriptRoot, locals);
            expression = expression.cast(scriptRoot, locals);
        }

        variable = locals.addVariable(location, resolvedType.getType(), name, false);
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeStatementOffset(location);

        if (expression == null) {
            Class<?> sort = variable.clazz;

            if (sort == void.class || sort == boolean.class || sort == byte.class ||
                sort == short.class || sort == char.class || sort == int.class) {
                methodWriter.push(0);
            } else if (sort == long.class) {
                methodWriter.push(0L);
            } else if (sort == float.class) {
                methodWriter.push(0F);
            } else if (sort == double.class) {
                methodWriter.push(0D);
            } else {
                methodWriter.visitInsn(Opcodes.ACONST_NULL);
            }
        } else {
            expression.write(classWriter, methodWriter, globals);
        }

        methodWriter.visitVarInsn(MethodWriter.getType(variable.clazz).getOpcode(Opcodes.ISTORE), variable.getSlot());
    }

    @Override
    public String toString() {
        if (expression == null) {
            return singleLineToString(type, name);
        }
        return singleLineToString(type, name, expression);
    }
}
