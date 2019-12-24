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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

import static org.elasticsearch.painless.Locals.Variable;

public class ForLoopNode extends LoopNode {

    /* ---- begin tree structure ---- */

    protected IRNode initializerNode;
    protected ExpressionNode afterthoughtNode;

    public ForLoopNode setInitialzerNode(IRNode initializerNode) {
        this.initializerNode = initializerNode;
        return this;
    }

    public IRNode getInitializerNode() {
        return initializerNode;
    }

    public ForLoopNode setAfterthoughtNode(ExpressionNode afterthoughtNode) {
        this.afterthoughtNode = afterthoughtNode;
        return this;
    }

    public ExpressionNode getAfterthoughtNode() {
        return afterthoughtNode;
    }

    @Override
    public ForLoopNode setConditionNode(ExpressionNode conditionNode) {
        super.setConditionNode(conditionNode);
        return this;
    }

    @Override
    public ForLoopNode setBlockNode(BlockNode blockNode) {
        super.setBlockNode(blockNode);
        return this;
    }

    /* ---- end tree structure, begin node data ---- */

    @Override
    public ForLoopNode setContinuous(boolean isContinuous) {
        super.setContinuous(isContinuous);
        return this;
    }

    @Override
    public ForLoopNode setLoopCounter(Variable loopCounter) {
        super.setLoopCounter(loopCounter);
        return this;
    }

    @Override
    public ForLoopNode setLocation(Location location) {
        super.setLocation(location);
        return this;
    }

    /* ---- end node data ---- */

    public ForLoopNode() {
        // do nothing
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeStatementOffset(location);

        Label start = new Label();
        Label begin = afterthoughtNode == null ? start : new Label();
        Label end = new Label();

        if (initializerNode instanceof DeclarationBlockNode) {
            initializerNode.write(classWriter, methodWriter, globals);
        } else if (initializerNode instanceof ExpressionNode) {
            ExpressionNode initializer = (ExpressionNode)this.initializerNode;
            initializer.write(classWriter, methodWriter, globals);
            methodWriter.writePop(MethodWriter.getType(initializer.getType()).getSize());
        }

        methodWriter.mark(start);

        if (conditionNode != null && isContinuous == false) {
            conditionNode.write(classWriter, methodWriter, globals);
            methodWriter.ifZCmp(Opcodes.IFEQ, end);
        }

        boolean allEscape = false;

        if (blockNode != null) {
            allEscape = blockNode.doAllEscape();

            int statementCount = Math.max(1, blockNode.statementCount);

            if (afterthoughtNode != null) {
                ++statementCount;
            }

            if (loopCounter != null) {
                methodWriter.writeLoopCounter(loopCounter.getSlot(), statementCount, location);
            }

            blockNode.continueLabel = begin;
            blockNode.breakLabel = end;
            blockNode.write(classWriter, methodWriter, globals);
        } else {
            if (loopCounter != null) {
                methodWriter.writeLoopCounter(loopCounter.getSlot(), 1, location);
            }
        }

        if (afterthoughtNode != null) {
            methodWriter.mark(begin);
            afterthoughtNode.write(classWriter, methodWriter, globals);
            methodWriter.writePop(MethodWriter.getType(afterthoughtNode.getType()).getSize());
        }

        if (afterthoughtNode != null || !allEscape) {
            methodWriter.goTo(start);
        }

        methodWriter.mark(end);
    }
}
