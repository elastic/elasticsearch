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
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ComparisonNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;

import java.util.Objects;

/**
 * Represents a comparison expression.
 */
public class EComp extends AExpression {

    private final AExpression leftNode;
    private final AExpression rightNode;
    private final Operation operation;

    public EComp(int identifier, Location location, AExpression leftNode, AExpression rightNode, Operation operation) {
        super(identifier, location);

        this.operation = Objects.requireNonNull(operation);
        this.leftNode = Objects.requireNonNull(leftNode);
        this.rightNode = Objects.requireNonNull(rightNode);
    }

    public AExpression getLeftNode() {
        return leftNode;
    }

    public AExpression getRightNode() {
        return rightNode;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: result not used from " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        Class<?> promotedType;

        Output output = new Output();

        Input leftInput = new Input();
        Output leftOutput = analyze(leftNode, classNode, semanticScope, leftInput);

        Input rightInput = new Input();
        Output rightOutput = analyze(rightNode, classNode, semanticScope, rightInput);

        if (operation == Operation.EQ || operation == Operation.EQR || operation == Operation.NE || operation == Operation.NER) {
            promotedType = AnalyzerCaster.promoteEquality(leftOutput.actual, rightOutput.actual);
        } else if (operation == Operation.GT || operation == Operation.GTE || operation == Operation.LT || operation == Operation.LTE) {
            promotedType = AnalyzerCaster.promoteNumeric(leftOutput.actual, rightOutput.actual, true);
        } else {
            throw createError(new IllegalStateException("unexpected binary operation [" + operation.name + "]"));
        }

        if (promotedType == null) {
            throw createError(new ClassCastException("cannot apply the " + operation.name + " operator " +
                    "[" + operation.symbol + "] to the types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(leftOutput.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(rightOutput.actual) + "]"));
        }

        if (operation != Operation.EQR && operation != Operation.NER && promotedType == def.class) {
            leftInput.expected = leftOutput.actual;
            rightInput.expected = rightOutput.actual;
        } else {
            leftInput.expected = promotedType;
            rightInput.expected = promotedType;
        }

        if ((operation == Operation.EQ || operation == Operation.EQR || operation == Operation.NE || operation == Operation.NER)
                && leftNode instanceof ENull && rightNode instanceof ENull) {
            throw createError(new IllegalArgumentException("extraneous comparison of [null] constants"));
        }

        PainlessCast leftCast = AnalyzerCaster.getLegalCast(leftNode.getLocation(),
                leftOutput.actual, leftInput.expected, leftInput.explicit, leftInput.internal);
        PainlessCast rightCast = AnalyzerCaster.getLegalCast(rightNode.getLocation(),
                rightOutput.actual, rightInput.expected, rightInput.explicit, rightInput.internal);

        output.actual = boolean.class;

        ComparisonNode comparisonNode = new ComparisonNode();

        comparisonNode.setLeftNode(cast(leftOutput.expressionNode, leftCast));
        comparisonNode.setRightNode(cast(rightOutput.expressionNode, rightCast));

        comparisonNode.setLocation(getLocation());
        comparisonNode.setExpressionType(output.actual);
        comparisonNode.setComparisonType(promotedType);
        comparisonNode.setOperation(operation);

        output.expressionNode = comparisonNode;

        return output;
    }
}
