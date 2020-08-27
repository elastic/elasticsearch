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
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.WriteScope;
import org.elasticsearch.painless.symbol.WriteScope.Variable;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

public class ForEachSubArrayNode extends LoopNode {

    /* ---- begin node data ---- */

    private Class<?> variableType;
    private String variableName;
    private PainlessCast cast;
    private Class<?> arrayType;
    private String arrayName;
    private Class<?> indexType;
    private String indexName;
    private Class<?> indexedType;

    public void setVariableType(Class<?> variableType) {
        this.variableType = variableType;
    }

    public Class<?> getVariableType() {
        return variableType;
    }

    public String getVariableCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(variableType);
    }

    public void setVariableName(String variableName) {
        this.variableName = variableName;
    }

    public String getVariableName() {
        return variableName;
    }

    public void setCast(PainlessCast cast) {
        this.cast = cast;
    }

    public PainlessCast getCast() {
        return cast;
    }

    public void setArrayType(Class<?> arrayType) {
        this.arrayType = arrayType;
    }

    public Class<?> getArrayType() {
        return arrayType;
    }

    public String getArrayCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(arrayType);
    }

    public void setArrayName(String arrayName) {
        this.arrayName = arrayName;
    }

    public String getArrayName() {
        return arrayName;
    }

    public void setIndexType(Class<?> indexType) {
        this.indexType = indexType;
    }

    public Class<?> getIndexType() {
        return indexType;
    }

    public String getIndexCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(indexType);
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
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

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitForEachSubArrayLoop(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        getConditionNode().visit(irTreeVisitor, scope);
        getBlockNode().visit(irTreeVisitor, scope);
    }

    /* ---- end visitor ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        methodWriter.writeStatementOffset(location);

        Variable variable = writeScope.defineVariable(variableType, variableName);
        Variable array = writeScope.defineInternalVariable(arrayType, arrayName);
        Variable index = writeScope.defineInternalVariable(indexType, indexName);

        getConditionNode().write(classWriter, methodWriter, writeScope);
        methodWriter.visitVarInsn(array.getAsmType().getOpcode(Opcodes.ISTORE), array.getSlot());
        methodWriter.push(-1);
        methodWriter.visitVarInsn(index.getAsmType().getOpcode(Opcodes.ISTORE), index.getSlot());

        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(begin);

        methodWriter.visitIincInsn(index.getSlot(), 1);
        methodWriter.visitVarInsn(index.getAsmType().getOpcode(Opcodes.ILOAD), index.getSlot());
        methodWriter.visitVarInsn(array.getAsmType().getOpcode(Opcodes.ILOAD), array.getSlot());
        methodWriter.arrayLength();
        methodWriter.ifICmp(MethodWriter.GE, end);

        methodWriter.visitVarInsn(array.getAsmType().getOpcode(Opcodes.ILOAD), array.getSlot());
        methodWriter.visitVarInsn(index.getAsmType().getOpcode(Opcodes.ILOAD), index.getSlot());
        methodWriter.arrayLoad(MethodWriter.getType(indexedType));
        methodWriter.writeCast(cast);
        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ISTORE), variable.getSlot());

        Variable loop = writeScope.getInternalVariable("loop");

        if (loop != null) {
            methodWriter.writeLoopCounter(loop.getSlot(), location);
        }

        getBlockNode().continueLabel = begin;
        getBlockNode().breakLabel = end;
        getBlockNode().write(classWriter, methodWriter, writeScope);

        methodWriter.goTo(begin);
        methodWriter.mark(end);
    }
}
