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
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.InstanceofNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;

/**
 * Represents {@code instanceof} operator.
 * <p>
 * Unlike java's, this works for primitive types too.
 */
public class EInstanceof extends AExpression {

    private final AExpression expressionNode;
    private final String canonicalTypeName;

    public EInstanceof(int identifier, Location location, AExpression expression, String canonicalTypeName) {
        super(identifier, location);

        this.expressionNode = Objects.requireNonNull(expression);
        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
    }

    public AExpression getExpressionNode() {
        return expressionNode;
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitInstanceof(this, input);
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to instanceof with target type [" + canonicalTypeName + "]"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: result not used from instanceof with target type [" + canonicalTypeName + "]"));
        }

        Class<?> resolvedType;
        Class<?> expressionType;
        boolean primitiveExpression;

        Output output = new Output();

        // ensure the specified type is part of the definition
        Class<?> clazz = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (clazz == null) {
            throw createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        // map to wrapped type for primitive types
        resolvedType = clazz.isPrimitive() ? PainlessLookupUtility.typeToBoxedType(clazz) :
                PainlessLookupUtility.typeToJavaType(clazz);

        // analyze and cast the expression
        semanticScope.setCondition(expressionNode, Read.class);
        Output expressionOutput = analyze(expressionNode, classNode, semanticScope);
        Class<?> expressionValueType = semanticScope.getDecoration(expressionNode, ValueType.class).getValueType();

        // record if the expression returns a primitive
        primitiveExpression = expressionValueType.isPrimitive();
        // map to wrapped type for primitive types
        expressionType = expressionValueType.isPrimitive() ?
            PainlessLookupUtility.typeToBoxedType(expressionValueType) : PainlessLookupUtility.typeToJavaType(clazz);

        semanticScope.putDecoration(this, new ValueType(boolean.class));

        InstanceofNode instanceofNode = new InstanceofNode();
        instanceofNode.setChildNode(expressionOutput.expressionNode);
        instanceofNode.setLocation(getLocation());
        instanceofNode.setExpressionType(boolean.class);
        instanceofNode.setInstanceType(expressionType);
        instanceofNode.setResolvedType(resolvedType);
        instanceofNode.setPrimitiveResult(primitiveExpression);
        output.expressionNode = instanceofNode;

        return output;
    }
}
