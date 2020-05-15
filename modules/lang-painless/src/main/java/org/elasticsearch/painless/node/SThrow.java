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
import org.elasticsearch.painless.ir.ThrowNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a throw statement.
 */
public class SThrow extends AStatement {

    protected final AExpression expression;

    public SThrow(Location location, AExpression expression) {
        super(location);

        this.expression = Objects.requireNonNull(expression);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        Output output = new Output();

        AExpression.Input expressionInput = new AExpression.Input();
        expressionInput.expected = Exception.class;
        AExpression.Output expressionOutput = AExpression.analyze(expression, classNode, scriptRoot, scope, expressionInput);
        PainlessCast expressionCast = AnalyzerCaster.getLegalCast(expression.location,
                expressionOutput.actual, expressionInput.expected, expressionInput.explicit, expressionInput.internal);

        output.methodEscape = true;
        output.loopEscape = true;
        output.allEscape = true;
        output.statementCount = 1;

        ThrowNode throwNode = new ThrowNode();
        throwNode.setExpressionNode(AExpression.cast(expressionOutput.expressionNode, expressionCast));
        throwNode.setLocation(location);

        output.statementNode = throwNode;

        return output;
    }
}
