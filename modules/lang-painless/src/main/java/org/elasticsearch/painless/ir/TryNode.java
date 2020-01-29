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
import org.elasticsearch.painless.symbol.ScopeTable;
import org.objectweb.asm.Label;

import java.util.ArrayList;
import java.util.List;

public class TryNode extends StatementNode {

    /* ---- begin tree structure ---- */

    private BlockNode blockNode;
    private final List<CatchNode> catchNodes = new ArrayList<>();

    public void setBlockNode(BlockNode blockNode) {
        this.blockNode = blockNode;
    }

    public BlockNode getBlockNode() {
        return blockNode;
    }

    public void addCatchNode(CatchNode catchNode) {
        catchNodes.add(catchNode);
    }

    public List<CatchNode> getCatchsNodes() {
        return catchNodes;
    }

    /* ---- end tree structure ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        methodWriter.writeStatementOffset(location);

        Label begin = new Label();
        Label end = new Label();
        Label exception = new Label();

        methodWriter.mark(begin);

        blockNode.continueLabel = continueLabel;
        blockNode.breakLabel = breakLabel;
        blockNode.write(classWriter, methodWriter, globals, scopeTable.newScope());

        if (blockNode.doAllEscape() == false) {
            methodWriter.goTo(exception);
        }

        methodWriter.mark(end);

        for (CatchNode catchNode : catchNodes) {
            catchNode.begin = begin;
            catchNode.end = end;
            catchNode.exception = catchNodes.size() > 1 ? exception : null;
            catchNode.write(classWriter, methodWriter, globals, scopeTable.newScope());
        }

        if (blockNode.doAllEscape() == false || catchNodes.size() > 1) {
            methodWriter.mark(exception);
        }
    }
}
