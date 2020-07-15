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
import org.elasticsearch.painless.symbol.Decorations.InLoop;
import org.elasticsearch.painless.symbol.Decorations.LoopEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.SemanticScope;

/**
 * Represents a for loop.
 */
public class SFor extends AStatement {

    private final ANode initializerNode;
    private final AExpression conditionNode;
    private final AExpression afterthoughtNode;
    private final SBlock blockNode;

    public SFor(int identifier, Location location,
            ANode initializerNode, AExpression conditionNode, AExpression afterthoughtNode, SBlock blockNode) {

        super(identifier, location);

        this.initializerNode = initializerNode;
        this.conditionNode = conditionNode;
        this.afterthoughtNode = afterthoughtNode;
        this.blockNode = blockNode;
    }

    public ANode getInitializerNode() {
        return initializerNode;
    }

    public AExpression getConditionNode() {
        return conditionNode;
    }

    public AExpression getAfterthoughtNode() {
        return afterthoughtNode;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitFor(this, input);
    }

    @Override
    void analyze(SemanticScope semanticScope) {
        semanticScope = semanticScope.newLocalScope();

        if (initializerNode != null) {
            if (initializerNode instanceof SDeclBlock) {
                ((SDeclBlock)initializerNode).analyze(semanticScope);
            } else if (initializerNode instanceof AExpression) {
                AExpression initializer = (AExpression)this.initializerNode;
                AExpression.analyze(initializer, semanticScope);
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        boolean continuous = false;

        if (conditionNode != null) {
            semanticScope.setCondition(conditionNode, Read.class);
            semanticScope.putDecoration(conditionNode, new TargetType(boolean.class));
            AExpression.analyze(conditionNode, semanticScope);
            conditionNode.cast(semanticScope);

            if (conditionNode instanceof EBooleanConstant) {
                continuous = ((EBooleanConstant)conditionNode).getBool();

                if (continuous == false) {
                    throw createError(new IllegalArgumentException("Extraneous for loop."));
                }

                if (blockNode == null) {
                    throw createError(new IllegalArgumentException("For loop has no escape."));
                }
            }
        } else {
            continuous = true;
        }

        if (afterthoughtNode != null) {
            AExpression.analyze(afterthoughtNode, semanticScope);
        }

        if (blockNode != null) {
            semanticScope.setCondition(blockNode, BeginLoop.class);
            semanticScope.setCondition(blockNode, InLoop.class);
            blockNode.analyze(semanticScope);

            if (semanticScope.getCondition(blockNode, LoopEscape.class) &&
                    semanticScope.getCondition(blockNode, AnyContinue.class) == false) {
                throw createError(new IllegalArgumentException("Extraneous for loop."));
            }

            if (continuous && semanticScope.getCondition(blockNode, AnyBreak.class) == false) {
                semanticScope.setCondition(this, MethodEscape.class);
                semanticScope.setCondition(this, AllEscape.class);
            }
        }
    }
}
