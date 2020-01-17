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
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StatementExpressionNode;
import org.elasticsearch.painless.ir.StatementNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents the top-level node for an expression as a statement.
 */
public final class SExpression extends AStatement {

    private AExpression expression;

    public SExpression(Location location, AExpression expression) {
        super(location);

        this.expression = Objects.requireNonNull(expression);
    }

    @Override
    Output analyze(ScriptRoot scriptRoot, Scope scope, Input input) {
        this.input = input;
        output = new Output();

        Class<?> rtnType = scope.getReturnType();
        boolean isVoid = rtnType == void.class;

        AExpression.Input expressionInput = new AExpression.Input();
        expressionInput.read = input.lastSource && !isVoid;
        AExpression.Output expressionOutput = expression.analyze(scriptRoot, scope, expressionInput);

        if ((input.lastSource == false || isVoid) && expressionOutput.statement == false) {
            throw createError(new IllegalArgumentException("Not a statement."));
        }

        boolean rtn = input.lastSource && isVoid == false && expressionOutput.actual != void.class;

        expression.input.expected = rtn ? rtnType : expressionOutput.actual;
        expression.input.internal = rtn;
        expression.cast();

        output.methodEscape = rtn;
        output.loopEscape = rtn;
        output.allEscape = rtn;
        output.statementCount = 1;

        return output;
    }

    @Override
    StatementNode write(ClassNode classNode) {
        ExpressionNode expressionNode = expression.cast(expression.write(classNode));

        if (output.methodEscape) {
            ReturnNode returnNode = new ReturnNode();

            returnNode.setExpressionNode(expressionNode);

            returnNode.setLocation(location);

            return returnNode;
        } else {
            StatementExpressionNode statementExpressionNode = new StatementExpressionNode();

            statementExpressionNode.setExpressionNode(expressionNode);

            statementExpressionNode.setLocation(location);

            return statementExpressionNode;
        }
    }

    @Override
    public String toString() {
        return singleLineToString(expression);
    }
}
