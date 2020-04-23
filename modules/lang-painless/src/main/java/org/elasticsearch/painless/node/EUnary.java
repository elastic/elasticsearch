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
import org.elasticsearch.painless.phase.DefaultSemanticAnalysisPhase;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.Negate;
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

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, EUnary userUnaryNode, SemanticScope semanticScope) {

        Operation operation = userUnaryNode.getOperation();

        if (semanticScope.getCondition(userUnaryNode, Write.class)) {
            throw userUnaryNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        if (semanticScope.getCondition(userUnaryNode, Read.class) == false) {
            throw userUnaryNode.createError(new IllegalArgumentException(
                    "not a statement: result not used from " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        AExpression userChildNode = userUnaryNode.getChildNode();
        Class<?> valueType;
        Class<?> unaryType = null;

        if (operation == Operation.SUB && (userChildNode instanceof ENumeric || userChildNode instanceof EDecimal)) {
            semanticScope.setCondition(userChildNode, Read.class);
            semanticScope.copyDecoration(userUnaryNode, userChildNode, TargetType.class);
            semanticScope.replicateCondition(userUnaryNode, userChildNode, Explicit.class);
            semanticScope.replicateCondition(userUnaryNode, userChildNode, Internal.class);
            semanticScope.setCondition(userChildNode, Negate.class);
            visitor.checkedVisit(userChildNode, semanticScope);

            if (semanticScope.hasDecoration(userUnaryNode, TargetType.class)) {
                visitor.decorateWithCast(userChildNode, semanticScope);
            }

            valueType = semanticScope.getDecoration(userChildNode, ValueType.class).getValueType();
        } else {
            if (operation == Operation.NOT) {
                semanticScope.setCondition(userChildNode, Read.class);
                semanticScope.putDecoration(userChildNode, new TargetType(boolean.class));
                visitor.checkedVisit(userChildNode, semanticScope);
                visitor.decorateWithCast(userChildNode, semanticScope);

                valueType = boolean.class;
            } else if (operation == Operation.BWNOT || operation == Operation.ADD || operation == Operation.SUB) {
                semanticScope.setCondition(userChildNode, Read.class);
                visitor.checkedVisit(userChildNode, semanticScope);
                Class<?> childValueType = semanticScope.getDecoration(userChildNode, ValueType.class).getValueType();

                unaryType = AnalyzerCaster.promoteNumeric(childValueType, operation != Operation.BWNOT);

                if (unaryType == null) {
                    throw userUnaryNode.createError(new ClassCastException("cannot apply the " + operation.name + " operator " +
                            "[" + operation.symbol + "] to the type " +
                            "[" + PainlessLookupUtility.typeToCanonicalTypeName(childValueType) + "]"));
                }

                semanticScope.putDecoration(userChildNode, new TargetType(unaryType));
                visitor.decorateWithCast(userChildNode, semanticScope);

                TargetType targetType = semanticScope.getDecoration(userUnaryNode, TargetType.class);

                if (unaryType == def.class && targetType != null) {
                    valueType = targetType.getTargetType();
                } else {
                    valueType = unaryType;
                }
            } else {
                throw userUnaryNode.createError(new IllegalStateException("unexpected unary operation [" + operation.name + "]"));
            }
        }

        semanticScope.putDecoration(userUnaryNode, new ValueType(valueType));

        if (unaryType != null) {
            semanticScope.putDecoration(userUnaryNode, new UnaryType(unaryType));
        }
    }
}
