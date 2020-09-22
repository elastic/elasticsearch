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
import org.elasticsearch.painless.symbol.WriteScope.Variable;
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

    /* ---- end tree structure, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitForLoop(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        if (initializerNode != null) {
            initializerNode.visit(irTreeVisitor, scope);
        }

        if (getConditionNode() != null) {
            getConditionNode().visit(irTreeVisitor, scope);
        }

        if (afterthoughtNode != null) {
            afterthoughtNode.visit(irTreeVisitor, scope);
        }

        if (getBlockNode() != null) {
            getBlockNode().visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        methodWriter.writeStatementOffset(location);

        writeScope = writeScope.newScope();

        Label start = new Label();
        Label begin = afterthoughtNode == null ? start : new Label();
        Label end = new Label();

        if (initializerNode instanceof DeclarationBlockNode) {
            initializerNode.write(classWriter, methodWriter, writeScope);
        } else if (initializerNode instanceof ExpressionNode) {
            ExpressionNode initializer = (ExpressionNode)this.initializerNode;

            initializer.write(classWriter, methodWriter, writeScope);
            methodWriter.writePop(MethodWriter.getType(initializer.getExpressionType()).getSize());
        }

        methodWriter.mark(start);

        if (getConditionNode() != null && isContinuous() == false) {
            getConditionNode().write(classWriter, methodWriter, writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, end);
        }

        Variable loop = writeScope.getInternalVariable("loop");

        if (loop != null) {
            methodWriter.writeLoopCounter(loop.getSlot(), location);
        }

        boolean allEscape = false;

        if (getBlockNode() != null) {
            allEscape = getBlockNode().doAllEscape();

            getBlockNode().continueLabel = begin;
            getBlockNode().breakLabel = end;
            getBlockNode().write(classWriter, methodWriter, writeScope);
        }

        if (afterthoughtNode != null) {
            methodWriter.mark(begin);
            afterthoughtNode.write(classWriter, methodWriter, writeScope);
            methodWriter.writePop(MethodWriter.getType(afterthoughtNode.getExpressionType()).getSize());
        }

        if (afterthoughtNode != null || allEscape == false) {
            methodWriter.goTo(start);
        }

        methodWriter.mark(end);
    }
}
