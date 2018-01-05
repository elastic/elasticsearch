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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.Objects;
import java.util.Set;

/**
 * Represents {@code instanceof} operator.
 * <p>
 * Unlike java's, this works for primitive types too.
 */
public final class EInstanceof extends AExpression {
    private AExpression expression;
    private final String type;

    private Class<?> resolvedType;
    private Class<?> expressionType;
    private boolean primitiveExpression;

    public EInstanceof(Location location, AExpression expression, String type) {
        super(location);
        this.expression = Objects.requireNonNull(expression);
        this.type = Objects.requireNonNull(type);
    }

    @Override
    void extractVariables(Set<String> variables) {
        expression.extractVariables(variables);
    }

    @Override
    void analyze(Locals locals) {
        final Type type;

        // ensure the specified type is part of the definition
        try {
            type = locals.getDefinition().getType(this.type);
        } catch (IllegalArgumentException exception) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        // map to wrapped type for primitive types
        resolvedType = type.clazz.isPrimitive() ? locals.getDefinition().getBoxedType(type).clazz : type.clazz;

        // analyze and cast the expression
        expression.analyze(locals);
        expression.expected = expression.actual;
        expression = expression.cast(locals);

        // record if the expression returns a primitive
        primitiveExpression = expression.actual.clazz.isPrimitive();
        // map to wrapped type for primitive types
        expressionType = expression.actual.clazz.isPrimitive() ? locals.getDefinition().getBoxedType(expression.actual).clazz : type.clazz;

        actual = locals.getDefinition().booleanType;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        // primitive types
        if (primitiveExpression) {
            // run the expression anyway (who knows what it does)
            expression.write(writer, globals);
            // discard its result
            writer.writePop(expression.actual.type.getSize());
            // push our result: its a primitive so it cannot be null.
            writer.push(resolvedType.isAssignableFrom(expressionType));
        } else {
            // ordinary instanceof
            expression.write(writer, globals);
            writer.instanceOf(org.objectweb.asm.Type.getType(resolvedType));
        }
    }

    @Override
    public String toString() {
        return singleLineToString(expression, type);
    }
}
