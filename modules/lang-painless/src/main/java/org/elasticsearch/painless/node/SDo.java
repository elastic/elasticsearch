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
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.AllEscape;
import org.elasticsearch.painless.symbol.Decorations.AnyBreak;
import org.elasticsearch.painless.symbol.Decorations.AnyContinue;
import org.elasticsearch.painless.symbol.Decorations.BeginLoop;
import org.elasticsearch.painless.symbol.Decorations.ContinuousLoop;
import org.elasticsearch.painless.symbol.Decorations.InLoop;
import org.elasticsearch.painless.symbol.Decorations.LoopEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;

/**
 * Represents a do-while loop.
 */
public class SDo extends AStatement {

    private final AExpression conditionNode;
    private final SBlock blockNode;

    public SDo(int identifier, Location location, AExpression conditionNode, SBlock blockNode) {
        super(identifier, location);

        this.conditionNode = Objects.requireNonNull(conditionNode);
        this.blockNode = blockNode;
    }

    public AExpression getConditionNode() {
        return conditionNode;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitDo(this, input);
    }

    @Override
    void analyze(SemanticScope semanticScope) {
        semanticScope = semanticScope.newLocalScope();

        if (blockNode == null) {
            throw createError(new IllegalArgumentException("Extraneous do while loop."));
        }

        semanticScope.setCondition(blockNode, BeginLoop.class);
        semanticScope.setCondition(blockNode, InLoop.class);
        blockNode.analyze(semanticScope);

        if (semanticScope.getCondition(blockNode, LoopEscape.class) &&
                semanticScope.getCondition(blockNode, AnyContinue.class) == false) {
            throw createError(new IllegalArgumentException("Extraneous do while loop."));
        }

        semanticScope.setCondition(conditionNode, Read.class);
        semanticScope.putDecoration(conditionNode, new TargetType(boolean.class));
        AExpression.analyze(conditionNode, semanticScope);
        conditionNode.cast(semanticScope);

        boolean continuous;

        if (conditionNode instanceof EBooleanConstant) {
            continuous = ((EBooleanConstant)conditionNode).getBool();

            if (continuous == false) {
                throw createError(new IllegalArgumentException("Extraneous do while loop."));
            } else {
                semanticScope.setCondition(this, ContinuousLoop.class);
            }

            if (semanticScope.getCondition(blockNode, AnyBreak.class) == false) {
                semanticScope.setCondition(this, MethodEscape.class);
                semanticScope.setCondition(this, AllEscape.class);
            }
        }
    }
}
