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
import org.elasticsearch.painless.symbol.Decorations.AnyBreak;
import org.elasticsearch.painless.symbol.Decorations.AnyContinue;
import org.elasticsearch.painless.symbol.Decorations.InLoop;
import org.elasticsearch.painless.symbol.Decorations.LastLoop;
import org.elasticsearch.painless.symbol.Decorations.LastSource;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;

/**
 * Represents an if block.
 */
public class SIf extends AStatement {

    private final AExpression conditionNode;
    private final SBlock ifBlockNode;

    public SIf(int identifier, Location location, AExpression conditionNode, SBlock ifBlockNode) {
        super(identifier, location);

        this.conditionNode = Objects.requireNonNull(conditionNode);
        this.ifBlockNode = ifBlockNode;
    }

    public AExpression getConditionNode() {
        return conditionNode;
    }

    public SBlock getIfBlockNode() {
        return ifBlockNode;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitIf(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, SIf userIfNode, SemanticScope semanticScope) {

        AExpression userConditionNode = userIfNode.getConditionNode();
        semanticScope.setCondition(userConditionNode, Read.class);
        semanticScope.putDecoration(userConditionNode, new TargetType(boolean.class));
        visitor.checkedVisit(userConditionNode, semanticScope);
        visitor.decorateWithCast(userConditionNode, semanticScope);

        SBlock userIfBlockNode = userIfNode.getIfBlockNode();

        if (userConditionNode instanceof EBooleanConstant || userIfBlockNode == null) {
            throw userIfNode.createError(new IllegalArgumentException("extraneous if block"));
        }

        semanticScope.replicateCondition(userIfNode, userIfBlockNode, LastSource.class);
        semanticScope.replicateCondition(userIfNode, userIfBlockNode, InLoop.class);
        semanticScope.replicateCondition(userIfNode, userIfBlockNode, LastLoop.class);
        visitor.visit(userIfBlockNode, semanticScope.newLocalScope());
        semanticScope.replicateCondition(userIfBlockNode, userIfNode, AnyContinue.class);
        semanticScope.replicateCondition(userIfBlockNode, userIfNode, AnyBreak.class);
    }
}
