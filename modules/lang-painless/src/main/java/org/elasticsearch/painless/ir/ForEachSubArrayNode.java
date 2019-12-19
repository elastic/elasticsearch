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
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

public class ForEachSubArrayNode extends LoopNode {

    /* ---- being tree structure ---- */

    protected TypeNode indexedTypeNode;

    public void setIndexedTypeNode(TypeNode indexedTypeNode) {
        this.indexedTypeNode = indexedTypeNode;
    }

    public TypeNode getIndexedTypeNode() {
        return indexedTypeNode;
    }

    public Class<?> getIndexedType() {
        return indexedTypeNode.getType();
    }

    public String getIndexedCanonicalTypeName() {
        return indexedTypeNode.getCanonicalTypeName();
    }

    /* ---- begin node data ---- */

    protected Variable variable;
    protected PainlessCast cast;
    protected Variable array;
    protected Variable index;

    public ForEachSubArrayNode setVariable(Variable variable) {
        this.variable = variable;
        return this;
    }

    public Variable getVariable() {
        return this.variable;
    }

    public ForEachSubArrayNode setCast(PainlessCast cast) {
        this.cast = cast;
        return this;
    }

    public PainlessCast getCast() {
        return cast;
    }

    public ForEachSubArrayNode setArray(Variable array) {
        this.array = array;
        return this;
    }

    public Variable getArray() {
        return this.array;
    }

    public ForEachSubArrayNode setIndex(Variable index) {
        this.index = index;
        return this;
    }

    public Variable getIndex() {
        return this.index;
    }

    /* ---- end node data ---- */

    ForEachSubArrayNode() {
        // do nothing
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeStatementOffset(location);

        conditionNode.write(classWriter, methodWriter, globals);
        methodWriter.visitVarInsn(MethodWriter.getType(array.clazz).getOpcode(Opcodes.ISTORE), array.getSlot());
        methodWriter.push(-1);
        methodWriter.visitVarInsn(MethodWriter.getType(index.clazz).getOpcode(Opcodes.ISTORE), index.getSlot());

        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(begin);

        methodWriter.visitIincInsn(index.getSlot(), 1);
        methodWriter.visitVarInsn(MethodWriter.getType(index.clazz).getOpcode(Opcodes.ILOAD), index.getSlot());
        methodWriter.visitVarInsn(MethodWriter.getType(array.clazz).getOpcode(Opcodes.ILOAD), array.getSlot());
        methodWriter.arrayLength();
        methodWriter.ifICmp(MethodWriter.GE, end);

        methodWriter.visitVarInsn(MethodWriter.getType(array.clazz).getOpcode(Opcodes.ILOAD), array.getSlot());
        methodWriter.visitVarInsn(MethodWriter.getType(index.clazz).getOpcode(Opcodes.ILOAD), index.getSlot());
        methodWriter.arrayLoad(MethodWriter.getType(getIndexedType()));
        methodWriter.writeCast(cast);
        methodWriter.visitVarInsn(MethodWriter.getType(variable.clazz).getOpcode(Opcodes.ISTORE), variable.getSlot());

        if (loopCounter != null) {
            methodWriter.writeLoopCounter(loopCounter.getSlot(), blockNode.getStatementCount(), location);
        }

        blockNode.continueLabel = begin;
        blockNode.breakLabel = end;
        blockNode.write(classWriter, methodWriter, globals);

        methodWriter.goTo(begin);
        methodWriter.mark(end);
    }
}
