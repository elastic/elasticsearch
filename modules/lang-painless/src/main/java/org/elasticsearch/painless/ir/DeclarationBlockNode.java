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

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.MethodWriter;

import java.util.ArrayList;
import java.util.List;

public final class DeclarationBlockNode extends StatementNode {

    /* ---- begin tree structure ---- */

    protected List<DeclarationNode> declarationNodes = new ArrayList<>();

    public DeclarationBlockNode addStatementNode(DeclarationNode declarationNode) {
        declarationNodes.add(declarationNode);
        return this;
    }

    public DeclarationBlockNode setStatementNode(int index, DeclarationNode declarationNode) {
        declarationNodes.set(index, declarationNode);
        return this;
    }

    public DeclarationNode getStatementNode(int index) {
        return declarationNodes.get(index);
    }

    public DeclarationBlockNode removeStatementNode(DeclarationNode declarationNode) {
        declarationNodes.remove(declarationNode);
        return this;
    }

    public DeclarationBlockNode removeStatementNode(int index) {
        declarationNodes.remove(index);
        return this;
    }

    public int getStatementsSize() {
        return declarationNodes.size();
    }

    public List<DeclarationNode> getStatementsNodes() {
        return declarationNodes;
    }

    public DeclarationBlockNode clearStatementNodes() {
        declarationNodes.clear();
        return this;
    }

    /* ---- end tree structure ---- */

    public DeclarationBlockNode() {
        // do nothing
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        for (DeclarationNode declarationNode : declarationNodes) {
            declarationNode.write(classWriter, methodWriter, globals);
        }
    }
}
