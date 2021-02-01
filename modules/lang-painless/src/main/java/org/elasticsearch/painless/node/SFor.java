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
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitFor(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        if (initializerNode != null) {
            initializerNode.visit(userTreeVisitor, scope);
        }

        if (conditionNode != null) {
            conditionNode.visit(userTreeVisitor, scope);
        }

        if (afterthoughtNode != null) {
            afterthoughtNode.visit(userTreeVisitor, scope);
        }

        if (blockNode != null) {
            blockNode.visit(userTreeVisitor, scope);
        }
    }
}
