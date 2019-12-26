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

import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.ir.WhileNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a while loop.
 */
public final class SWhile extends AStatement {

    private AExpression condition;
    private final SBlock block;

    private boolean continuous = false;

    public SWhile(Location location, AExpression condition, SBlock block) {
        super(location);

        this.condition = Objects.requireNonNull(condition);
        this.block = block;
    }

    @Override
    void extractVariables(Set<String> variables) {
        condition.extractVariables(variables);
        if (block != null) {
            block.extractVariables(variables);
        }
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        locals = Locals.newLocalScope(locals);

        condition.expected = boolean.class;
        condition.analyze(scriptRoot, locals);
        condition = condition.cast(scriptRoot, locals);

        if (condition.constant != null) {
            continuous = (boolean)condition.constant;

            if (!continuous) {
                throw createError(new IllegalArgumentException("Extraneous while loop."));
            }

            if (block == null) {
                throw createError(new IllegalArgumentException("While loop has no escape."));
            }
        }

        if (block != null) {
            block.beginLoop = true;
            block.inLoop = true;

            block.analyze(scriptRoot, locals);

            if (block.loopEscape && !block.anyContinue) {
                throw createError(new IllegalArgumentException("Extraneous while loop."));
            }

            if (continuous && !block.anyBreak) {
                methodEscape = true;
                allEscape = true;
            }

            block.statementCount = Math.max(1, block.statementCount);
        }

        statementCount = 1;

        if (locals.hasVariable(Locals.LOOP)) {
            loopCounter = locals.getVariable(location, Locals.LOOP);
        }
    }

    @Override
    WhileNode write() {
        WhileNode whileNode = new WhileNode();

        whileNode.setConditionNode(condition.write());
        whileNode.setBlockNode(block == null ? null : block.write());

        whileNode.setLocation(location);
        whileNode.setContinuous(continuous);

        return whileNode;
    }

    @Override
    public String toString() {
        return singleLineToString(condition, block);
    }
}
