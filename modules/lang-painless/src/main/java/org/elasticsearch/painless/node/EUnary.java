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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.UnaryType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;

/**
 * Represents a unary math expression.
 */
public class EUnary extends AExpression {

    private final AExpression childNode;
    private final Operation operation;

    public EUnary(int identifier, Location location, AExpression childNode, Operation operation) {
        super(identifier, location);

        this.childNode = Objects.requireNonNull(childNode);
        this.operation = Objects.requireNonNull(operation);
    }

    public AExpression getChildNode() {
        return childNode;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitUnary(this, input);
    }

    @Override
    void analyze(SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: result not used from " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        Class<?> valueType;
        Class<?> promote = null;

        if ((operation == Operation.SUB || operation == Operation.ADD) &&
                (childNode instanceof ENumeric || childNode instanceof EDecimal)) {
            semanticScope.setCondition(childNode, Read.class);
            semanticScope.copyDecoration(this, childNode, TargetType.class);
            semanticScope.replicateCondition(this, childNode, Explicit.class);
            semanticScope.replicateCondition(this, childNode, Internal.class);

            if (childNode instanceof ENumeric) {
                ENumeric numeric = (ENumeric)childNode;

                if (operation == Operation.SUB) {
                    numeric.analyze(semanticScope, numeric.getNumeric().charAt(0) != '-');
                } else {
                    childNode.analyze(semanticScope);
                }
            } else if (childNode instanceof EDecimal) {
                EDecimal decimal = (EDecimal)childNode;

                if (operation == Operation.SUB) {
                    decimal.analyze(semanticScope, decimal.getDecimal().charAt(0) != '-');
                } else {
                    childNode.analyze(semanticScope);
                }
            } else {
                throw createError(new IllegalArgumentException("illegal tree structure"));
            }

            valueType = semanticScope.getDecoration(childNode, ValueType.class).getValueType();
        } else {
            if (operation == Operation.NOT) {
                semanticScope.setCondition(childNode, Read.class);
                semanticScope.putDecoration(childNode, new TargetType(boolean.class));
                analyze(childNode, semanticScope);
                childNode.cast(semanticScope);

                valueType = boolean.class;
            } else if (operation == Operation.BWNOT || operation == Operation.ADD || operation == Operation.SUB) {
                semanticScope.setCondition(childNode, Read.class);
                analyze(childNode, semanticScope);
                Class<?> childValueType = semanticScope.getDecoration(childNode, ValueType.class).getValueType();

                promote = AnalyzerCaster.promoteNumeric(childValueType, operation != Operation.BWNOT);

                if (promote == null) {
                    throw createError(new ClassCastException("cannot apply the " + operation.name + " operator " +
                            "[" + operation.symbol + "] to the type " +
                            "[" + PainlessLookupUtility.typeToCanonicalTypeName(childValueType) + "]"));
                }

                semanticScope.putDecoration(childNode, new TargetType(promote));
                childNode.cast(semanticScope);

                TargetType targetType = semanticScope.getDecoration(this, TargetType.class);

                if (promote == def.class && targetType != null) {
                    valueType = targetType.getTargetType();
                } else {
                    valueType = promote;
                }
            } else {
                throw createError(new IllegalStateException("unexpected unary operation [" + operation.name + "]"));
            }
        }

        semanticScope.putDecoration(this, new ValueType(valueType));

        if (promote != null) {
            semanticScope.putDecoration(this, new UnaryType(promote));
        }
    }
}
