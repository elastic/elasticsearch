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

    @Override
    void analyze(SemanticScope semanticScope) {
        if (blockNode == null) {
            throw createError(new IllegalArgumentException("Extraneous try statement."));
        }

        semanticScope.replicateCondition(this, blockNode, LastSource.class);
        semanticScope.replicateCondition(this, blockNode, InLoop.class);
        semanticScope.replicateCondition(this, blockNode, LastLoop.class);
        blockNode.analyze(semanticScope.newLocalScope());

        boolean methodEscape = semanticScope.getCondition(blockNode, MethodEscape.class);
        boolean loopEscape = semanticScope.getCondition(blockNode, LoopEscape.class);
        boolean allEscape = semanticScope.getCondition(blockNode, AllEscape.class);
        boolean anyContinue = semanticScope.getCondition(blockNode, AnyContinue.class);
        boolean anyBreak = semanticScope.getCondition(blockNode, AnyBreak.class);

        for (SCatch catc : catchNodes) {
            semanticScope.replicateCondition(this, catc, LastSource.class);
            semanticScope.replicateCondition(this, catc, InLoop.class);
            semanticScope.replicateCondition(this, catc, LastLoop.class);
            catc.analyze(semanticScope.newLocalScope());

            methodEscape &= semanticScope.getCondition(catc, MethodEscape.class);
            loopEscape &= semanticScope.getCondition(catc, LoopEscape.class);
            allEscape &= semanticScope.getCondition(catc, AllEscape.class);
            anyContinue |= semanticScope.getCondition(catc, AnyContinue.class);
            anyBreak |= semanticScope.getCondition(catc, AnyBreak.class);
        }

        if (methodEscape) {
            semanticScope.setCondition(this, MethodEscape.class);
        }

        if (loopEscape) {
            semanticScope.setCondition(this, LoopEscape.class);
        }

        if (allEscape) {
            semanticScope.setCondition(this, AllEscape.class);
        }

        if (anyContinue) {
            semanticScope.setCondition(this, AnyContinue.class);
        }

        if (anyBreak) {
            semanticScope.setCondition(this, AnyBreak.class);
        }
    }
}
