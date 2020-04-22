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
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents the try block as part of a try-catch block.
 */
public class STry extends AStatement {

    private final SBlock blockNode;
    private final List<SCatch> catchNodes;

    public STry(int identifier, Location location, SBlock blockNode, List<SCatch> catchNodes) {
        super(identifier, location);

        this.blockNode = blockNode;
        this.catchNodes = Collections.unmodifiableList(Objects.requireNonNull(catchNodes));
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    public List<SCatch> getCatchNodes() {
        return catchNodes;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitTry(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, STry userTryNode, SemanticScope semanticScope) {

        SBlock userBlockNode = userTryNode.getBlockNode();

        if (userBlockNode == null) {
            throw userTryNode.createError(new IllegalArgumentException("extraneous try statement"));
        }

        semanticScope.replicateCondition(userTryNode, userBlockNode, LastSource.class);
        semanticScope.replicateCondition(userTryNode, userBlockNode, InLoop.class);
        semanticScope.replicateCondition(userTryNode, userBlockNode, LastLoop.class);
        visitor.visit(userBlockNode, semanticScope.newLocalScope());

        boolean methodEscape = semanticScope.getCondition(userBlockNode, MethodEscape.class);
        boolean loopEscape = semanticScope.getCondition(userBlockNode, LoopEscape.class);
        boolean allEscape = semanticScope.getCondition(userBlockNode, AllEscape.class);
        boolean anyContinue = semanticScope.getCondition(userBlockNode, AnyContinue.class);
        boolean anyBreak = semanticScope.getCondition(userBlockNode, AnyBreak.class);

        for (SCatch userCatchNode : userTryNode.getCatchNodes()) {
            semanticScope.replicateCondition(userTryNode, userCatchNode, LastSource.class);
            semanticScope.replicateCondition(userTryNode, userCatchNode, InLoop.class);
            semanticScope.replicateCondition(userTryNode, userCatchNode, LastLoop.class);
            visitor.visit(userCatchNode, semanticScope.newLocalScope());

            methodEscape &= semanticScope.getCondition(userCatchNode, MethodEscape.class);
            loopEscape &= semanticScope.getCondition(userCatchNode, LoopEscape.class);
            allEscape &= semanticScope.getCondition(userCatchNode, AllEscape.class);
            anyContinue |= semanticScope.getCondition(userCatchNode, AnyContinue.class);
            anyBreak |= semanticScope.getCondition(userCatchNode, AnyBreak.class);
        }

        if (methodEscape) {
            semanticScope.setCondition(userTryNode, MethodEscape.class);
        }

        if (loopEscape) {
            semanticScope.setCondition(userTryNode, LoopEscape.class);
        }

        if (allEscape) {
            semanticScope.setCondition(userTryNode, AllEscape.class);
        }

        if (anyContinue) {
            semanticScope.setCondition(userTryNode, AnyContinue.class);
        }

        if (anyBreak) {
            semanticScope.setCondition(userTryNode, AnyBreak.class);
        }
    }
}
