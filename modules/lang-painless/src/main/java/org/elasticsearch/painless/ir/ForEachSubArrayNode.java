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
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

public class ForEachSubArrayNode extends LoopNode {

    /* ---- begin node data ---- */

    private Variable variable;
    private PainlessCast cast;
    private Variable array;
    private Variable index;
    private Class<?> indexedType;

    public void setVariable(Variable variable) {
        this.variable = variable;
    }

    public Variable getVariable() {
        return variable;
    }

    public void setCast(PainlessCast cast) {
        this.cast = cast;
    }

    public PainlessCast getCast() {
        return cast;
    }

    public void setArray(Variable array) {
        this.array = array;
    }

    public Variable getArray() {
        return array;
    }

    public void setIndex(Variable index) {
        this.index = index;
    }

    public Variable getIndex() {
        return index;
    }
    
    public void setIndexedType(Class<?> indexedType) {
        this.indexedType = indexedType;
    }

    public Class<?> getIndexedType() {
        return indexedType;
    }

    public String getIndexedCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(indexedType);
    }
    
    /* ---- end node data ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeStatementOffset(location);

        getConditionNode().write(classWriter, methodWriter, globals);
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

        if (getLoopCounter() != null) {
            methodWriter.writeLoopCounter(getLoopCounter().getSlot(), getBlockNode().getStatementCount(), location);
        }

        getBlockNode().continueLabel = begin;
        getBlockNode().breakLabel = end;
        getBlockNode().write(classWriter, methodWriter, globals);

        methodWriter.goTo(begin);
        methodWriter.mark(end);
    }
}
