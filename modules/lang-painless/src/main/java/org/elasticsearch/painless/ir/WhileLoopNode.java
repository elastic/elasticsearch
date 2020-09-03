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

public class WhileLoopNode extends LoopNode {

    /* ---- begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitWhileLoop(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        if (getConditionNode() != null) {
            getConditionNode().visit(irTreeVisitor, scope);
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

        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(begin);

        if (isContinuous() == false) {
            getConditionNode().write(classWriter, methodWriter, writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, end);
        }

        Variable loop = writeScope.getInternalVariable("loop");

        if (loop != null) {
            methodWriter.writeLoopCounter(loop.getSlot(), location);
        }

        if (getBlockNode() != null) {
            getBlockNode().continueLabel = begin;
            getBlockNode().breakLabel = end;
            getBlockNode().write(classWriter, methodWriter, writeScope);
        }

        if (getBlockNode() == null || getBlockNode().doAllEscape() == false) {
            methodWriter.goTo(begin);
        }

        methodWriter.mark(end);
    }
}
