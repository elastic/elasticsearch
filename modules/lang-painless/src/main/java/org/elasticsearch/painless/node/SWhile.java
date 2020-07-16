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
import org.elasticsearch.painless.symbol.Decorations.ContinuousLoop;
import org.elasticsearch.painless.symbol.Decorations.InLoop;
import org.elasticsearch.painless.symbol.Decorations.LoopEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;

/**
 * Represents a while loop.
 */
public class SWhile extends AStatement {

    private final AExpression conditionNode;
    private final SBlock blockNode;

    public SWhile(int identifier, Location location, AExpression conditionNode, SBlock blockNode) {
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
        return userTreeVisitor.visitWhile(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, SWhile userWhileNode, SemanticScope semanticScope) {

        semanticScope = semanticScope.newLocalScope();

        AExpression userConditionNode = userWhileNode.getConditionNode();
        semanticScope.setCondition(userConditionNode, Read.class);
        semanticScope.putDecoration(userConditionNode, new TargetType(boolean.class));
        visitor.checkedVisit(userConditionNode, semanticScope);
        visitor.decorateWithCast(userConditionNode, semanticScope);

        SBlock userBlockNode = userWhileNode.getBlockNode();
        boolean continuous = false;

        if (userConditionNode instanceof EBooleanConstant) {
            continuous = ((EBooleanConstant)userConditionNode).getBool();

            if (continuous == false) {
                throw userWhileNode.createError(new IllegalArgumentException("extraneous while loop"));
            } else {
                semanticScope.setCondition(userWhileNode, ContinuousLoop.class);
            }

            if (userBlockNode == null) {
                throw userWhileNode.createError(new IllegalArgumentException("no paths escape from while loop"));
            }
        }

        if (userBlockNode != null) {
            semanticScope.setCondition(userBlockNode, BeginLoop.class);
            semanticScope.setCondition(userBlockNode, InLoop.class);
            visitor.visit(userBlockNode, semanticScope);

            if (semanticScope.getCondition(userBlockNode, LoopEscape.class) &&
                    semanticScope.getCondition(userBlockNode, AnyContinue.class) == false) {
                throw userWhileNode.createError(new IllegalArgumentException("extraneous while loop"));
            }

            if (continuous && semanticScope.getCondition(userBlockNode, AnyBreak.class) == false) {
                semanticScope.setCondition(userWhileNode, MethodEscape.class);
                semanticScope.setCondition(userWhileNode, AllEscape.class);
            }
        }
    }
}
