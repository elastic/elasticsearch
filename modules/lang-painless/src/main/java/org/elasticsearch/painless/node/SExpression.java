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
import org.elasticsearch.painless.ir.StatementExpressionNode;
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
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        Class<?> rtnType = scope.getReturnType();
        boolean isVoid = rtnType == void.class;

        expression.read = lastSource && !isVoid;
        expression.analyze(scriptRoot, scope);

        if (!lastSource && !expression.statement) {
            throw createError(new IllegalArgumentException("Not a statement."));
        }

        boolean rtn = lastSource && !isVoid && expression.actual != void.class;

        expression.expected = rtn ? rtnType : expression.actual;
        expression.internal = rtn;
        expression = expression.cast(scriptRoot, scope);

        methodEscape = rtn;
        loopEscape = rtn;
        allEscape = rtn;
        statementCount = 1;
    }

    @Override
    StatementExpressionNode write(ClassNode classNode) {
        StatementExpressionNode statementExpressionNode = new StatementExpressionNode();

        statementExpressionNode.setExpressionNode(expression.write(classNode));

        statementExpressionNode.setLocation(location);
        statementExpressionNode.setMethodEscape(methodEscape);

        return statementExpressionNode;
    }

    @Override
    public String toString() {
        return singleLineToString(expression);
    }
}
