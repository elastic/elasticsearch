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
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.WriteScope;
import org.elasticsearch.painless.symbol.WriteScope.Variable;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

import java.util.Iterator;

import static org.elasticsearch.painless.WriterConstants.ITERATOR_HASNEXT;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_NEXT;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_TYPE;

/**
 * Represents a for-each loop for iterables.
 */
public class ForEachSubIterableNode extends LoopNode {

    /* ---- begin node data ---- */

    private Class<?> variableType;
    private String variableName;
    private PainlessCast cast;
    private Class<?> iteratorType;
    private String iteratorName;
    private PainlessMethod method;

    public void setVariableType(Class<?> variableType) {
        this.variableType = variableType;
    }

    public Class<?> getVariableType() {
        return variableType;
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

    public void setIteratorType(Class<?> iteratorType) {
        this.iteratorType = iteratorType;
    }

    public Class<?> getIteratorType() {
        return iteratorType;
    }

    public void setIteratorName(String iteratorName) {
        this.iteratorName = iteratorName;
    }

    public String getIteratorName() {
        return iteratorName;
    }

    public void setMethod(PainlessMethod method) {
        this.method = method;
    }

    public PainlessMethod getMethod() {
        return method;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitForEachSubIterableLoop(this, scope);
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
        Variable iterator = writeScope.defineInternalVariable(iteratorType, iteratorName);

        getConditionNode().write(classWriter, methodWriter, writeScope);

        if (method == null) {
            org.objectweb.asm.Type methodType = org.objectweb.asm.Type
                    .getMethodType(org.objectweb.asm.Type.getType(Iterator.class), org.objectweb.asm.Type.getType(Object.class));
            methodWriter.invokeDefCall("iterator", methodType, DefBootstrap.ITERATOR);
        } else {
            methodWriter.invokeMethodCall(method);
        }

        methodWriter.visitVarInsn(iterator.getAsmType().getOpcode(Opcodes.ISTORE), iterator.getSlot());

        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(begin);

        methodWriter.visitVarInsn(iterator.getAsmType().getOpcode(Opcodes.ILOAD), iterator.getSlot());
        methodWriter.invokeInterface(ITERATOR_TYPE, ITERATOR_HASNEXT);
        methodWriter.ifZCmp(MethodWriter.EQ, end);

        methodWriter.visitVarInsn(iterator.getAsmType().getOpcode(Opcodes.ILOAD), iterator.getSlot());
        methodWriter.invokeInterface(ITERATOR_TYPE, ITERATOR_NEXT);
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
