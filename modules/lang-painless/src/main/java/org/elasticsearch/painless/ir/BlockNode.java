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
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.WriteScope;

import java.util.ArrayList;
import java.util.List;

public class BlockNode extends StatementNode {

    /* ---- begin tree structure ---- */

    private final List<StatementNode> statementNodes = new ArrayList<>();

    public void addStatementNode(StatementNode statementNode) {
        statementNodes.add(statementNode);
    }

    public List<StatementNode> getStatementsNodes() {
        return statementNodes;
    }

    /* ---- end tree structure, begin node data ---- */

    private boolean doAllEscape;
    private int statementCount;

    public void setAllEscape(boolean doAllEscape) {
        this.doAllEscape = doAllEscape;
    }

    public boolean doAllEscape() {
        return doAllEscape;
    }

    public void setStatementCount(int statementCount) {
        this.statementCount = statementCount;
    }

    public int getStatementCount() {
        return statementCount;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitBlock(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        for (StatementNode statementNode : statementNodes) {
            statementNode.visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        for (StatementNode statementNode : statementNodes) {
            statementNode.continueLabel = continueLabel;
            statementNode.breakLabel = breakLabel;
            statementNode.write(classWriter, methodWriter, writeScope);
        }
    }
}
