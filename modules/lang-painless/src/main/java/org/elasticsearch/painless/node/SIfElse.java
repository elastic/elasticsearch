/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.UserTreeVisitor;

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
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitIfElse(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        conditionNode.visit(userTreeVisitor, scope);

        if (ifBlockNode != null) {
            ifBlockNode.visit(userTreeVisitor, scope);
        }

        if (elseBlockNode != null) {
            elseBlockNode.visit(userTreeVisitor, scope);
        }
    }
}
