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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;

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
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        // ensure the specified type is part of the definition
        Class<?> clazz = scriptRoot.getPainlessLookup().canonicalTypeNameToType(this.type);

        if (clazz == null) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        // map to wrapped type for primitive types
        resolvedType = clazz.isPrimitive() ? PainlessLookupUtility.typeToBoxedType(clazz) :
                PainlessLookupUtility.typeToJavaType(clazz);

        // analyze and cast the expression
        expression.analyze(scriptRoot, locals);
        expression.expected = expression.actual;
        expression = expression.cast(scriptRoot, locals);

        // record if the expression returns a primitive
        primitiveExpression = expression.actual.isPrimitive();
        // map to wrapped type for primitive types
        expressionType = expression.actual.isPrimitive() ?
            PainlessLookupUtility.typeToBoxedType(expression.actual) : PainlessLookupUtility.typeToJavaType(clazz);

        actual = boolean.class;
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        // primitive types
        if (primitiveExpression) {
            // run the expression anyway (who knows what it does)
            expression.write(classWriter, methodWriter, globals);
            // discard its result
            methodWriter.writePop(MethodWriter.getType(expression.actual).getSize());
            // push our result: its a primitive so it cannot be null.
            methodWriter.push(resolvedType.isAssignableFrom(expressionType));
        } else {
            // ordinary instanceof
            expression.write(classWriter, methodWriter, globals);
            methodWriter.instanceOf(org.objectweb.asm.Type.getType(resolvedType));
        }
    }

    @Override
    public String toString() {
        return singleLineToString(expression, type);
    }
}
