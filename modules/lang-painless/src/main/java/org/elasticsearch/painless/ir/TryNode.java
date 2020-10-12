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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.WriteScope;
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

    /* ---- end tree structure, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitTry(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        blockNode.visit(irTreeVisitor, scope);

        for (CatchNode catchNode : catchNodes) {
            catchNode.visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    public TryNode(Location location) {
        super(location);
    }

    @Override
    protected void write(WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(getLocation());

        Label tryBeginLabel = new Label();
        Label tryEndLabel = new Label();
        Label catchesEndLabel = new Label();

        methodWriter.mark(tryBeginLabel);

        blockNode.write(writeScope.newBlockScope());

        if (blockNode.doAllEscape() == false) {
            methodWriter.goTo(catchesEndLabel);
        }

        methodWriter.mark(tryEndLabel);

        for (int i = 0; i < catchNodes.size(); ++i) {
            CatchNode catchNode = catchNodes.get(i);
            Label catchJumpLabel = catchNodes.size() > 1 && i < catchNodes.size() - 1 ? catchesEndLabel : null;
            catchNode.write(writeScope.newTryScope(tryBeginLabel, tryEndLabel, catchJumpLabel));
        }

        if (blockNode.doAllEscape() == false || catchNodes.size() > 1) {
            methodWriter.mark(catchesEndLabel);
        }
    }
}
