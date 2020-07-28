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

import static java.util.Objects.requireNonNull;

/**
 * The Elvis operator ({@code ?:}), a null coalescing operator. Binary operator that evaluates the first expression and return it if it is
 * non null. If the first expression is null then it evaluates the second expression and returns it.
 */
public class EElvis extends AExpression {

    private final AExpression leftNode;
    private final AExpression rightNode;

    public EElvis(int identifier, Location location, AExpression leftNode, AExpression rightNode) {
        super(identifier, location);

        this.leftNode = requireNonNull(leftNode);
        this.rightNode = requireNonNull(rightNode);
    }

    public AExpression getLeftNode() {
        return leftNode;
    }

    public AExpression getRightNode() {
        return rightNode;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitElvis(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        leftNode.visit(userTreeVisitor, scope);
        rightNode.visit(userTreeVisitor, scope);
    }
}
