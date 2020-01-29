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
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.util.ArrayList;
import java.util.List;

public class FunctionNode extends IRNode {

    /* ---- begin tree structure ---- */

    private BlockNode blockNode;

    public void setBlockNode(BlockNode blockNode) {
        this.blockNode = blockNode;
    }

    public BlockNode getBlockNode() {
        return blockNode;
    }

    /* ---- end tree structure, begin node data ---- */

    private String name;
    private Class<?> returnType;
    private List<Class<?>> typeParameters = new ArrayList<>();
    private List<String> parameterNames = new ArrayList<>();
    private boolean isSynthetic;
    private boolean doesMethodEscape;
    private int maxLoopCounter;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setReturnType(Class<?> returnType) {
        this.returnType = returnType;
    }

    public Class<?> getReturnType() {
        return returnType;
    }

    public void addTypeParameter(Class<?> typeParameter) {
        typeParameters.add(typeParameter);
    }

    public List<Class<?>> getTypeParameters() {
        return typeParameters;
    }

    public void addParameterName(String parameterName) {
        parameterNames.add(parameterName);
    }

    public List<String> getParameterNames() {
        return parameterNames;
    }

    public void setSynthetic(boolean isSythetic) {
        this.isSynthetic = isSythetic;
    }

    public boolean isSynthetic() {
        return isSynthetic;
    }

    public void setMethodEscape(boolean doesMethodEscape) {
        this.doesMethodEscape = doesMethodEscape;
    }

    public boolean doesMethodEscape() {
        return doesMethodEscape;
    }

    public void setMaxLoopCounter(int maxLoopCounter) {
        this.maxLoopCounter = maxLoopCounter;
    }

    public int getMaxLoopCounter() {
        return maxLoopCounter;
    }

    /* ---- end node data ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        int access = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;

        if (isSynthetic) {
            access |= Opcodes.ACC_SYNTHETIC;
        }

        Type asmReturnType = MethodWriter.getType(returnType);
        Type[] asmParameterTypes = new Type[typeParameters.size()];

        for (int index = 0; index < asmParameterTypes.length; ++index) {
            Class<?> type = typeParameters.get(index);
            String name = parameterNames.get(index);
            scopeTable.defineVariable(type, name);
            asmParameterTypes[index] = MethodWriter.getType(typeParameters.get(index));
        }

        Method method = new Method(name, asmReturnType, asmParameterTypes);

        methodWriter = classWriter.newMethodWriter(access, method);
        methodWriter.visitCode();

        if (maxLoopCounter > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()

            Variable loop = scopeTable.defineInternalVariable(int.class, "loop");

            methodWriter.push(maxLoopCounter);
            methodWriter.visitVarInsn(Opcodes.ISTORE, loop.getSlot());
        }

        blockNode.write(classWriter, methodWriter, globals, scopeTable.newScope());

        if (doesMethodEscape == false) {
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
