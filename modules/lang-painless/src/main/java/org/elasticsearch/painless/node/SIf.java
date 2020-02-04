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
import org.elasticsearch.painless.ir.IfNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents an if block.
 */
public final class SIf extends AStatement {

    AExpression condition;
    final SBlock ifblock;

    public SIf(Location location, AExpression condition, SBlock ifblock) {
        super(location);

        this.condition = Objects.requireNonNull(condition);
        this.ifblock = ifblock;
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        condition.expected = boolean.class;
        condition.analyze(scriptRoot, scope);
        condition = condition.cast(scriptRoot, scope);

        if (condition.constant != null) {
            throw createError(new IllegalArgumentException("Extraneous if statement."));
        }

        if (ifblock == null) {
            throw createError(new IllegalArgumentException("Extraneous if statement."));
        }

        ifblock.lastSource = lastSource;
        ifblock.inLoop = inLoop;
        ifblock.lastLoop = lastLoop;

        ifblock.analyze(scriptRoot, scope.newLocalScope());

        anyContinue = ifblock.anyContinue;
        anyBreak = ifblock.anyBreak;
        statementCount = ifblock.statementCount;
    }

    @Override
    IfNode write(ClassNode classNode) {
        IfNode ifNode = new IfNode();

        ifNode.setConditionNode(condition.write(classNode));
        ifNode.setBlockNode(ifblock.write(classNode));

        ifNode.setLocation(location);

        return ifNode;
    }

    @Override
    public String toString() {
        return singleLineToString(condition, ifblock);
    }
}
