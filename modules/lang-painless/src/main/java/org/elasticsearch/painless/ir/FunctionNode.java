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
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.util.List;

public class FunctionNode extends IRNode {

    /* ---- begin tree structure ---- */

    protected BlockNode blockNode;

    public FunctionNode setBlockNode(BlockNode blockNode) {
        this.blockNode = blockNode;
        return this;
    }

    public BlockNode getBlockNode() {
        return blockNode;
    }

    /* ---- end tree structure, begin node data ---- */

    protected String name;
    Class<?> returnType;
    List<Class<?>> typeParameters;
    protected boolean isSynthetic;
    protected boolean doesMethodEscape;
    protected Variable loopCounter;
    protected int maxLoopCounter;

    public FunctionNode setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public FunctionNode setReturnType(Class<?> returnType) {
        this.returnType = returnType;
        return this;
    }

    public Class<?> getReturnType() {
        return returnType;
    }

    public FunctionNode addTypeParameter(Class<?> typeParameter) {
        typeParameters.add(typeParameter);
        return this;
    }

    public FunctionNode setTypeParameter(int index, Class<?> typeParameter) {
        typeParameters.set(index, typeParameter);
        return this;
    }

    public Class<?> getTypeParameter(int index) {
        return typeParameters.get(index);
    }

    public FunctionNode removeTypeParameter(Class<?> typeParameter) {
        typeParameters.remove(typeParameter);
        return this;
    }

    public FunctionNode removeTypeParameter(int index) {
        typeParameters.remove(index);
        return this;
    }

    public int getTypeParametersSize() {
        return typeParameters.size();
    }

    public List<Class<?>> getTypeParameters() {
        return typeParameters;
    }

    public FunctionNode clearTypeParameters() {
        typeParameters.clear();
        return this;
    }

    public FunctionNode setSynthetic(boolean isSythetic) {
        this.name = name;
        return this;
    }

    public boolean isSynthetic() {
        return isSynthetic;
    }

    public FunctionNode setMethodEscape(boolean doesMethodEscape) {
        this.doesMethodEscape = doesMethodEscape;
        return this;
    }

    public boolean doesMethodEscape() {
        return doesMethodEscape;
    }

    public FunctionNode setLoopCounter(Variable loopCounter) {
        this.loopCounter = loopCounter;
        return this;
    }

    public Variable getLoopCounter() {
        return loopCounter;
    }

    public FunctionNode setMaxLoopCounter(int maxLoopCounter) {
        this.maxLoopCounter = maxLoopCounter;
        return this;
    }

    public int getMaxLoopCounter() {
        return maxLoopCounter;
    }

    /* ---- end node data ---- */

    public FunctionNode() {
        // do nothing
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        int access = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;

        if (isSynthetic) {
            access |= Opcodes.ACC_SYNTHETIC;
        }

        Type asmReturnType = Type.getType(returnType);
        Type[] asmParameterTypes = new Type[typeParameters.size()];

        for (int index = 0; index < asmParameterTypes.length; ++index) {
            asmParameterTypes[index] = Type.getType(typeParameters.get(index));
        }

        Method method = new Method(name, asmReturnType, asmParameterTypes);

        methodWriter = classWriter.newMethodWriter(access, method);
        methodWriter.visitCode();

        if (maxLoopCounter > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()
            methodWriter.push(maxLoopCounter);
            methodWriter.visitVarInsn(Opcodes.ISTORE, loopCounter.getSlot());
        }

        blockNode.write(classWriter, methodWriter, globals);

        if (!doesMethodEscape) {
            if (returnType == void.class) {
                methodWriter.returnValue();
            } else {
                throw new IllegalStateException("not all paths provide a return value " +
                        "for method [" + name + "] with [" + typeParameters.size() + "] parameters");
            }
        }

        methodWriter.endMethod();
    }
}
