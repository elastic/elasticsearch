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
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConditionalNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a conditional expression.
 */
public class EConditional extends AExpression {

    protected final AExpression condition;
    protected final AExpression left;
    protected final AExpression right;

    public EConditional(Location location, AExpression condition, AExpression left, AExpression right) {
        super(location);

        this.condition = Objects.requireNonNull(condition);
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException("invalid assignment: cannot assign a value to conditional operation [?:]"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException("not a statement: result not used from conditional operation [?:]"));
        }

        Output output = new Output();

        Input conditionInput = new Input();
        conditionInput.expected = boolean.class;
        Output conditionOutput = analyze(condition, classNode, scriptRoot, scope, conditionInput);
        PainlessCast conditionCast = AnalyzerCaster.getLegalCast(condition.location,
                conditionOutput.actual, conditionInput.expected, conditionInput.explicit, conditionInput.internal);

        Input leftInput = new Input();
        leftInput.expected = input.expected;
        leftInput.explicit = input.explicit;
        leftInput.internal = input.internal;
        Output leftOutput = analyze(left, classNode, scriptRoot, scope, leftInput);

        Input rightInput = new Input();
        rightInput.expected = input.expected;
        rightInput.explicit = input.explicit;
        rightInput.internal = input.internal;
        Output rightOutput = analyze(right, classNode, scriptRoot, scope, rightInput);

        output.actual = input.expected;

        if (input.expected == null) {
            Class<?> promote = AnalyzerCaster.promoteConditional(leftOutput.actual, rightOutput.actual);

            if (promote == null) {
                throw createError(new ClassCastException("cannot apply the conditional operator [?:] to the types " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(leftOutput.actual) + "] and " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(rightOutput.actual) + "]"));
            }

            leftInput.expected = promote;
            rightInput.expected = promote;
            output.actual = promote;
        }

        PainlessCast leftCast = AnalyzerCaster.getLegalCast(left.location,
                leftOutput.actual, leftInput.expected, leftInput.explicit, leftInput.internal);
        PainlessCast rightCast = AnalyzerCaster.getLegalCast(right.location,
                rightOutput.actual, rightInput.expected, rightInput.explicit, rightInput.internal);

        ConditionalNode conditionalNode = new ConditionalNode();

        conditionalNode.setLeftNode(cast(leftOutput.expressionNode, leftCast));
        conditionalNode.setRightNode(cast(rightOutput.expressionNode, rightCast));
        conditionalNode.setConditionNode(cast(conditionOutput.expressionNode, conditionCast));

        conditionalNode.setLocation(location);
        conditionalNode.setExpressionType(output.actual);

        output.expressionNode = conditionalNode;

        return output;
    }
}
