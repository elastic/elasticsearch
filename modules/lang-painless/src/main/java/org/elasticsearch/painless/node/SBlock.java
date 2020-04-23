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
import org.elasticsearch.painless.symbol.Decorations.LastLoop;
import org.elasticsearch.painless.symbol.Decorations.LastSource;
import org.elasticsearch.painless.symbol.Decorations.LoopEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a set of statements as a branch of control-flow.
 */
public class SBlock extends AStatement {

    private final List<AStatement> statementNodes;

    public SBlock(int identifier, Location location, List<AStatement> statementNodes) {
        super(identifier, location);

        this.statementNodes = Collections.unmodifiableList(Objects.requireNonNull(statementNodes));
    }

    public List<AStatement> getStatementNodes() {
        return statementNodes;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitBlock(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, SBlock userBlockNode, SemanticScope semanticScope) {

        List<AStatement> userStatementNodes = userBlockNode.getStatementNodes();

        if (userStatementNodes.isEmpty()) {
            throw userBlockNode.createError(new IllegalArgumentException("invalid block: found no statements"));
        }

        AStatement lastUserStatement = userStatementNodes.get(userStatementNodes.size() - 1);

        boolean lastSource = semanticScope.getCondition(userBlockNode, LastSource.class);
        boolean beginLoop = semanticScope.getCondition(userBlockNode, BeginLoop.class);
        boolean inLoop = semanticScope.getCondition(userBlockNode, InLoop.class);
        boolean lastLoop = semanticScope.getCondition(userBlockNode, LastLoop.class);

        boolean allEscape;
        boolean anyContinue = false;
        boolean anyBreak = false;

        for (AStatement userStatementNode : userStatementNodes) {
            if (inLoop) {
                semanticScope.setCondition(userStatementNode, InLoop.class);
            }

            if (userStatementNode == lastUserStatement) {
                if (beginLoop || lastLoop) {
                    semanticScope.setCondition(userStatementNode, LastLoop.class);
                }

                if (lastSource) {
                    semanticScope.setCondition(userStatementNode, LastSource.class);
                }
            }

            visitor.visit(userStatementNode, semanticScope);
            allEscape = semanticScope.getCondition(userStatementNode, AllEscape.class);

            if (userStatementNode == lastUserStatement) {
                semanticScope.replicateCondition(userStatementNode, userBlockNode, MethodEscape.class);
                semanticScope.replicateCondition(userStatementNode, userBlockNode, LoopEscape.class);

                if (allEscape) {
                    semanticScope.setCondition(userStatementNode, AllEscape.class);
                }
            } else {
                if (allEscape) {
                    throw userBlockNode.createError(new IllegalArgumentException("invalid block: unreachable statement"));
                }
            }

            anyContinue |= semanticScope.getCondition(userStatementNode, AnyContinue.class);
            anyBreak |= semanticScope.getCondition(userStatementNode, AnyBreak.class);
        }

        if (anyContinue) {
            semanticScope.setCondition(userBlockNode, AnyContinue.class);
        }

        if (anyBreak) {
            semanticScope.setCondition(userBlockNode, AnyBreak.class);
        }
    }
}
