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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.InstanceofNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents {@code instanceof} operator.
 * <p>
 * Unlike java's, this works for primitive types too.
 */
public final class EInstanceof extends AExpression {
    private AExpression expression;
    private final String type;

    private Class<?> resolvedType;
    private Class<?> instanceType;
    private boolean primitiveExpression;

    public EInstanceof(Location location, AExpression expression, String type) {
        super(location);
        this.expression = Objects.requireNonNull(expression);
        this.type = Objects.requireNonNull(type);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        // ensure the specified type is part of the definition
        Class<?> clazz = scriptRoot.getPainlessLookup().canonicalTypeNameToType(this.type);

        if (clazz == null) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        // map to wrapped type for primitive types
        resolvedType = clazz.isPrimitive() ? PainlessLookupUtility.typeToBoxedType(clazz) :
                PainlessLookupUtility.typeToJavaType(clazz);

        // analyze and cast the expression
        expression.analyze(scriptRoot, scope);
        expression.expected = expression.actual;
        expression = expression.cast(scriptRoot, scope);

        // record if the expression returns a primitive
        primitiveExpression = expression.actual.isPrimitive();
        // map to wrapped type for primitive types
        instanceType = expression.actual.isPrimitive() ?
            PainlessLookupUtility.typeToBoxedType(expression.actual) : PainlessLookupUtility.typeToJavaType(clazz);

        actual = boolean.class;
    }

    @Override
    InstanceofNode write(ClassNode classNode) {
        InstanceofNode instanceofNode = new InstanceofNode();

        instanceofNode.setChildNode(expression.write(classNode));

        instanceofNode.setLocation(location);
        instanceofNode.setExpressionType(actual);
        instanceofNode.setInstanceType(instanceType);
        instanceofNode.setResolvedType(resolvedType);
        instanceofNode.setPrimitiveResult(primitiveExpression);

        return instanceofNode;
    }

    @Override
    public String toString() {
        return singleLineToString(expression, type);
    }
}
