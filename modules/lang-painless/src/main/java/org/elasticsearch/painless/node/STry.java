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
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitTry(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        if (blockNode != null) {
            blockNode.visit(userTreeVisitor, scope);
        }

        for (SCatch catchNode : catchNodes) {
            catchNode.visit(userTreeVisitor, scope);
        }
    }
}
