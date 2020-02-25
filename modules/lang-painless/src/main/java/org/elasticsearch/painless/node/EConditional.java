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
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a conditional expression.
 */
public final class EConditional extends AExpression {

    private AExpression condition;
    private AExpression left;
    private AExpression right;

    public EConditional(Location location, AExpression condition, AExpression left, AExpression right) {
        super(location);

        this.condition = Objects.requireNonNull(condition);
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        condition.expected = boolean.class;
        condition.analyze(scriptRoot, scope);
        condition = condition.cast(scriptRoot, scope);

        if (condition.constant != null) {
            throw createError(new IllegalArgumentException("Extraneous conditional statement."));
        }

        left.expected = expected;
        left.explicit = explicit;
        left.internal = internal;
        right.expected = expected;
        right.explicit = explicit;
        right.internal = internal;
        actual = expected;

        left.analyze(scriptRoot, scope);
        right.analyze(scriptRoot, scope);

        if (expected == null) {
            Class<?> promote = AnalyzerCaster.promoteConditional(left.actual, right.actual, left.constant, right.constant);

            left.expected = promote;
            right.expected = promote;
            actual = promote;
        }

        left = left.cast(scriptRoot, scope);
        right = right.cast(scriptRoot, scope);
    }

    @Override
    ConditionalNode write(ClassNode classNode) {
        ConditionalNode conditionalNode = new ConditionalNode();

        conditionalNode.setLeftNode(left.write(classNode));
        conditionalNode.setRightNode(right.write(classNode));
        conditionalNode.setConditionNode(condition.write(classNode));

        conditionalNode.setLocation(location);
        conditionalNode.setExpressionType(actual);

        return conditionalNode;
    }

    @Override
    public String toString() {
        return singleLineToString(condition, left, right);
    }
}
