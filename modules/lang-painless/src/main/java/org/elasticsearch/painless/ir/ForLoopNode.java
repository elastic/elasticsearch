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
import org.elasticsearch.painless.symbol.ScopeTable.Variable;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

public class ForLoopNode extends LoopNode {

    /* ---- begin tree structure ---- */

    private IRNode initializerNode;
    private ExpressionNode afterthoughtNode;

    public void setInitialzerNode(IRNode initializerNode) {
        this.initializerNode = initializerNode;
    }

    public IRNode getInitializerNode() {
        return initializerNode;
    }

    public void setAfterthoughtNode(ExpressionNode afterthoughtNode) {
        this.afterthoughtNode = afterthoughtNode;
    }

    public ExpressionNode getAfterthoughtNode() {
        return afterthoughtNode;
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        methodWriter.writeStatementOffset(location);

        scopeTable = scopeTable.newScope();

        Label start = new Label();
        Label begin = afterthoughtNode == null ? start : new Label();
        Label end = new Label();

        if (initializerNode instanceof DeclarationBlockNode) {
            initializerNode.write(classWriter, methodWriter, globals, scopeTable);
        } else if (initializerNode instanceof ExpressionNode) {
            ExpressionNode initializer = (ExpressionNode)this.initializerNode;

            initializer.write(classWriter, methodWriter, globals, scopeTable);
            methodWriter.writePop(MethodWriter.getType(initializer.getExpressionType()).getSize());
        }

        methodWriter.mark(start);

        if (getConditionNode() != null && isContinuous() == false) {
            getConditionNode().write(classWriter, methodWriter, globals, scopeTable);
            methodWriter.ifZCmp(Opcodes.IFEQ, end);
        }

        boolean allEscape = false;

        if (getBlockNode() != null) {
            allEscape = getBlockNode().doAllEscape();

            int statementCount = Math.max(1, getBlockNode().getStatementCount());

            if (afterthoughtNode != null) {
                ++statementCount;
            }

            Variable loop = scopeTable.getInternalVariable("loop");

            if (loop != null) {
                methodWriter.writeLoopCounter(loop.getSlot(), statementCount, location);
            }

            getBlockNode().continueLabel = begin;
            getBlockNode().breakLabel = end;
            getBlockNode().write(classWriter, methodWriter, globals, scopeTable);
        } else {
            Variable loop = scopeTable.getInternalVariable("loop");

            if (loop != null) {
                methodWriter.writeLoopCounter(loop.getSlot(), 1, location);
            }
        }

        if (afterthoughtNode != null) {
            methodWriter.mark(begin);
            afterthoughtNode.write(classWriter, methodWriter, globals, scopeTable);
            methodWriter.writePop(MethodWriter.getType(afterthoughtNode.getExpressionType()).getSize());
        }

        if (afterthoughtNode != null || allEscape == false) {
            methodWriter.goTo(start);
        }

        methodWriter.mark(end);
    }
}
