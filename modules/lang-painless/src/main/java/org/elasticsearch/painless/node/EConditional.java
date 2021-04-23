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
 * Represents a conditional expression.
 */
public class EConditional extends AExpression {

    private final AExpression conditionNode;
    private final AExpression trueNode;
    private final AExpression falseNode;

    public EConditional(int identifier, Location location, AExpression conditionNode, AExpression trueNode, AExpression falseNode) {
        super(identifier, location);

        this.conditionNode = Objects.requireNonNull(conditionNode);
        this.trueNode = Objects.requireNonNull(trueNode);
        this.falseNode = Objects.requireNonNull(falseNode);
    }

    public AExpression getConditionNode() {
        return conditionNode;
    }

    public AExpression getTrueNode() {
        return trueNode;
    }

    public AExpression getFalseNode() {
        return falseNode;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitConditional(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        conditionNode.visit(userTreeVisitor, scope);
        trueNode.visit(userTreeVisitor, scope);
        falseNode.visit(userTreeVisitor, scope);
    }
}
