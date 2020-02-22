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
import org.elasticsearch.painless.ir.ThrowNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a throw statement.
 */
public final class SThrow extends AStatement {

    private AExpression expression;

    public SThrow(Location location, AExpression expression) {
        super(location);

        this.expression = Objects.requireNonNull(expression);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        expression.expected = Exception.class;
        expression.analyze(scriptRoot, scope);
        expression = expression.cast(scriptRoot, scope);

        methodEscape = true;
        loopEscape = true;
        allEscape = true;
        statementCount = 1;
    }

    @Override
    ThrowNode write(ClassNode classNode) {
        ThrowNode throwNode = new ThrowNode();

        throwNode.setExpressionNode(expression.write(classNode));

        throwNode.setLocation(location);

        return throwNode;
    }

    @Override
    public String toString() {
        return singleLineToString(expression);
    }
}
