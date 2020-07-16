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

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, SDo userDoNode, SemanticScope semanticScope) {

        semanticScope = semanticScope.newLocalScope();

        SBlock userBlockNode = userDoNode.getBlockNode();

        if (userBlockNode == null) {
            throw userDoNode.createError(new IllegalArgumentException("extraneous do-while loop"));
        }

        semanticScope.setCondition(userBlockNode, BeginLoop.class);
        semanticScope.setCondition(userBlockNode, InLoop.class);
        visitor.visit(userBlockNode, semanticScope);

        if (semanticScope.getCondition(userBlockNode, LoopEscape.class) &&
                semanticScope.getCondition(userBlockNode, AnyContinue.class) == false) {
            throw userDoNode.createError(new IllegalArgumentException("extraneous do-while loop"));
        }

        AExpression userConditionNode = userDoNode.getConditionNode();

        semanticScope.setCondition(userConditionNode, Read.class);
        semanticScope.putDecoration(userConditionNode, new TargetType(boolean.class));
        visitor.checkedVisit(userConditionNode, semanticScope);
        visitor.decorateWithCast(userConditionNode, semanticScope);

        boolean continuous;

        if (userConditionNode instanceof EBooleanConstant) {
            continuous = ((EBooleanConstant)userConditionNode).getBool();

            if (continuous == false) {
                throw userDoNode.createError(new IllegalArgumentException("extraneous do-while loop"));
            } else {
                semanticScope.setCondition(userDoNode, ContinuousLoop.class);
            }

            if (semanticScope.getCondition(userBlockNode, AnyBreak.class) == false) {
                semanticScope.setCondition(userDoNode, MethodEscape.class);
                semanticScope.setCondition(userDoNode, AllEscape.class);
            }
        }
    }
}
