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

import java.util.Objects;

/**
 * Represents a catch block as part of a try-catch block.
 */
public class SCatch extends AStatement {

    private final Class<?> baseException;
    private final String canonicalTypeName;
    private final String symbol;
    private final SBlock blockNode;

    public SCatch(int identifier, Location location, Class<?> baseException, String canonicalTypeName, String symbol, SBlock blockNode) {
        super(identifier, location);

        this.baseException = Objects.requireNonNull(baseException);
        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.symbol = Objects.requireNonNull(symbol);
        this.blockNode = blockNode;
    }

    public Class<?> getBaseException() {
        return baseException;
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public String getSymbol() {
        return symbol;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitCatch(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        if (blockNode != null) {
            blockNode.visit(userTreeVisitor, scope);
        }
    }
}
