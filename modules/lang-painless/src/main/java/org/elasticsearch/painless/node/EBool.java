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
import org.elasticsearch.painless.ir.BooleanNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.lookup.PainlessCast;

import java.util.Objects;

/**
 * Represents a boolean expression.
 */
public class EBool extends AExpression {

    private final AExpression leftNode;
    private final AExpression rightNode;
    private final Operation operation;

    public EBool(int identifier, Location location, AExpression leftNode, AExpression rightNode, Operation operation) {
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

        Output output = new Output();

        Input leftInput = new Input();
        leftInput.expected = boolean.class;
        Output leftOutput = analyze(leftNode, classNode, semanticScope, leftInput);
        PainlessCast leftCast = AnalyzerCaster.getLegalCast(leftNode.getLocation(),
                leftOutput.actual, leftInput.expected, leftInput.explicit, leftInput.internal);

        Input rightInput = new Input();
        rightInput.expected = boolean.class;
        Output rightOutput = analyze(rightNode, classNode, semanticScope, rightInput);
        PainlessCast rightCast = AnalyzerCaster.getLegalCast(rightNode.getLocation(),
                rightOutput.actual, rightInput.expected, rightInput.explicit, rightInput.internal);

        output.actual = boolean.class;

        BooleanNode booleanNode = new BooleanNode();

        booleanNode.setLeftNode(cast(leftOutput.expressionNode, leftCast));
        booleanNode.setRightNode(cast(rightOutput.expressionNode, rightCast));

        booleanNode.setLocation(getLocation());
        booleanNode.setExpressionType(output.actual);
        booleanNode.setOperation(operation);

        output.expressionNode = booleanNode;

        return output;
    }
}
