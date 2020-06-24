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
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StatementExpressionNode;
import org.elasticsearch.painless.lookup.PainlessCast;

import java.util.Objects;

/**
 * Represents the top-level node for an expression as a statement.
 */
public class SExpression extends AStatement {

    private final AExpression expressionNode;

    public SExpression(int identifier, Location location, AExpression expressionNode) {
        super(identifier, location);

        this.expressionNode = Objects.requireNonNull(expressionNode);
    }

    public AExpression getExpressionNode() {
        return expressionNode;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        Class<?> rtnType = semanticScope.getReturnType();
        boolean isVoid = rtnType == void.class;

        AExpression.Input expressionInput = new AExpression.Input();
        expressionInput.read = input.lastSource && !isVoid;
        AExpression.Output expressionOutput = AExpression.analyze(expressionNode, classNode, semanticScope, expressionInput);

        boolean rtn = input.lastSource && isVoid == false && expressionOutput.actual != void.class;

        expressionInput.expected = rtn ? rtnType : expressionOutput.actual;
        expressionInput.internal = rtn;
        PainlessCast expressionCast = AnalyzerCaster.getLegalCast(expressionNode.getLocation(),
                expressionOutput.actual, expressionInput.expected, expressionInput.explicit, expressionInput.internal);

        Output output = new Output();
        output.methodEscape = rtn;
        output.loopEscape = rtn;
        output.allEscape = rtn;
        output.statementCount = 1;

        ExpressionNode expressionNode = AExpression.cast(expressionOutput.expressionNode, expressionCast);

        if (output.methodEscape) {
            ReturnNode returnNode = new ReturnNode();
            returnNode.setExpressionNode(expressionNode);
            returnNode.setLocation(getLocation());

            output.statementNode = returnNode;
        } else {
            StatementExpressionNode statementExpressionNode = new StatementExpressionNode();
            statementExpressionNode.setExpressionNode(expressionNode);
            statementExpressionNode.setLocation(getLocation());

            output.statementNode = statementExpressionNode;
        }

        return output;
    }
}
