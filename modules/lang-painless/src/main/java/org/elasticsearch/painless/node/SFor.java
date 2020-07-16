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
import org.elasticsearch.painless.phase.DefaultSemanticAnalysisPhase;
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

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, SFor userForNode, SemanticScope semanticScope) {

        semanticScope = semanticScope.newLocalScope();

        ANode userInitializerNode = userForNode.getInitializerNode();

        if (userInitializerNode != null) {
            if (userInitializerNode instanceof SDeclBlock) {
                visitor.visit(userInitializerNode, semanticScope);
            } else if (userInitializerNode instanceof AExpression) {
                visitor.checkedVisit((AExpression)userInitializerNode, semanticScope);
            } else {
                throw userForNode.createError(new IllegalStateException("illegal tree structure"));
            }
        }

        AExpression userConditionNode = userForNode.getConditionNode();
        SBlock userBlockNode = userForNode.getBlockNode();
        boolean continuous = false;

        if (userConditionNode != null) {
            semanticScope.setCondition(userConditionNode, Read.class);
            semanticScope.putDecoration(userConditionNode, new TargetType(boolean.class));
            visitor.checkedVisit(userConditionNode, semanticScope);
            visitor.decorateWithCast(userConditionNode, semanticScope);

            if (userConditionNode instanceof EBooleanConstant) {
                continuous = ((EBooleanConstant)userConditionNode).getBool();

                if (continuous == false) {
                    throw userForNode.createError(new IllegalArgumentException("extraneous for loop"));
                }

                if (userBlockNode == null) {
                    throw userForNode.createError(new IllegalArgumentException("no paths escape from for loop"));
                }
            }
        } else {
            continuous = true;
        }

        AExpression userAfterthoughtNode = userForNode.getAfterthoughtNode();

        if (userAfterthoughtNode != null) {
            visitor.checkedVisit(userAfterthoughtNode, semanticScope);
        }

        if (userBlockNode != null) {
            semanticScope.setCondition(userBlockNode, BeginLoop.class);
            semanticScope.setCondition(userBlockNode, InLoop.class);
            visitor.visit(userBlockNode, semanticScope);

            if (semanticScope.getCondition(userBlockNode, LoopEscape.class) &&
                    semanticScope.getCondition(userBlockNode, AnyContinue.class) == false) {
                throw userForNode.createError(new IllegalArgumentException("extraneous for loop"));
            }

            if (continuous && semanticScope.getCondition(userBlockNode, AnyBreak.class) == false) {
                semanticScope.setCondition(userForNode, MethodEscape.class);
                semanticScope.setCondition(userForNode, AllEscape.class);
            }
        }
    }
}
