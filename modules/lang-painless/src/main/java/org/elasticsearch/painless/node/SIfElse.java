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
import org.elasticsearch.painless.symbol.Decorations.InLoop;
import org.elasticsearch.painless.symbol.Decorations.LastLoop;
import org.elasticsearch.painless.symbol.Decorations.LastSource;
import org.elasticsearch.painless.symbol.Decorations.LoopEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;

/**
 * Represents an if/else block.
 */
public class SIfElse extends AStatement {

    private final AExpression conditionNode;
    private final SBlock ifBlockNode;
    private final SBlock elseBlockNode;

    public SIfElse(int identifier, Location location, AExpression conditionNode, SBlock ifBlockNode, SBlock elseBlockNode) {
        super(identifier, location);

        this.conditionNode = Objects.requireNonNull(conditionNode);
        this.ifBlockNode = ifBlockNode;
        this.elseBlockNode = elseBlockNode;
    }

    public AExpression getConditionNode() {
        return conditionNode;
    }

    public SBlock getIfBlockNode() {
        return ifBlockNode;
    }

    public SBlock getElseBlockNode() {
        return elseBlockNode;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitIfElse(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, SIfElse userIfElseNode, SemanticScope semanticScope) {

        AExpression userConditionNode = userIfElseNode.getConditionNode();
        semanticScope.setCondition(userConditionNode, Read.class);
        semanticScope.putDecoration(userConditionNode, new TargetType(boolean.class));
        visitor.checkedVisit(userConditionNode, semanticScope);
        visitor.decorateWithCast(userConditionNode, semanticScope);

        SBlock userIfBlockNode = userIfElseNode.getIfBlockNode();

        if (userConditionNode instanceof EBooleanConstant || userIfBlockNode == null) {
            throw userIfElseNode.createError(new IllegalArgumentException("extraneous if block"));
        }

        semanticScope.replicateCondition(userIfElseNode, userIfBlockNode, LastSource.class);
        semanticScope.replicateCondition(userIfElseNode, userIfBlockNode, InLoop.class);
        semanticScope.replicateCondition(userIfElseNode, userIfBlockNode, LastLoop.class);
        visitor.visit(userIfBlockNode, semanticScope.newLocalScope());

        SBlock userElseBlockNode = userIfElseNode.getElseBlockNode();

        if (userElseBlockNode == null) {
            throw userIfElseNode.createError(new IllegalArgumentException("extraneous else block."));
        }

        semanticScope.replicateCondition(userIfElseNode, userElseBlockNode, LastSource.class);
        semanticScope.replicateCondition(userIfElseNode, userElseBlockNode, InLoop.class);
        semanticScope.replicateCondition(userIfElseNode, userElseBlockNode, LastLoop.class);
        visitor.visit(userElseBlockNode, semanticScope.newLocalScope());

        if (semanticScope.getCondition(userIfBlockNode, MethodEscape.class) &&
                semanticScope.getCondition(userElseBlockNode, MethodEscape.class)) {
            semanticScope.setCondition(userIfElseNode, MethodEscape.class);
        }

        if (semanticScope.getCondition(userIfBlockNode, LoopEscape.class) &&
                semanticScope.getCondition(userElseBlockNode, LoopEscape.class)) {
            semanticScope.setCondition(userIfElseNode, LoopEscape.class);
        }

        if (semanticScope.getCondition(userIfBlockNode, AllEscape.class) &&
                semanticScope.getCondition(userElseBlockNode, AllEscape.class)) {
            semanticScope.setCondition(userIfElseNode, AllEscape.class);
        }

        if (semanticScope.getCondition(userIfBlockNode, AnyContinue.class) ||
                semanticScope.getCondition(userElseBlockNode, AnyContinue.class)) {
            semanticScope.setCondition(userIfElseNode, AnyContinue.class);
        }

        if (semanticScope.getCondition(userIfBlockNode, AnyBreak.class) ||
                semanticScope.getCondition(userElseBlockNode, AnyBreak.class)) {
            semanticScope.setCondition(userIfElseNode, AnyBreak.class);
        }
    }
}
