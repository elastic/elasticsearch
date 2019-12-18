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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.List;

public class CallDefNode extends ArgumentsNode {

    /* ---- begin node data ---- */

    protected String name;
    protected String recipe;
    protected List<String> pointers;
    protected List<Class<?>> parameterTypes;

    public CallDefNode setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public CallDefNode setRecipe(String recipe) {
        this.recipe = recipe;
        return this;
    }

    public String getRecipe() {
        return recipe;
    }

    public CallDefNode addPointer(String pointer) {
        pointers.add(pointer);
        return this;
    }

    public CallDefNode setPointer(int index, String pointer) {
        pointers.set(index, pointer);
        return this;
    }

    public String getPointer(int index) {
        return pointers.get(index);
    }

    public CallDefNode removePointer(String pointer) {
        pointers.remove(pointer);
        return this;
    }

    public CallDefNode removePointer(int index) {
        pointers.remove(index);
        return this;
    }

    public int getPointersSize() {
        return pointers.size();
    }

    public List<String> getPointers() {
        return pointers;
    }

    public CallDefNode clearPointers() {
        pointers.clear();
        return this;
    }

    public CallDefNode addParameterType(Class<?> parameterType) {
        parameterTypes.add(parameterType);
        return this;
    }

    public CallDefNode setParameterType(int index, Class<?> parameterType) {
        parameterTypes.set(index, parameterType);
        return this;
    }

    public Class<?> getParameterType(int index) {
        return parameterTypes.get(index);
    }

    public CallDefNode removeParameterType(Class<?> parameterType) {
        parameterTypes.remove(parameterType);
        return this;
    }

    public CallDefNode removeParameterType(int index) {
        parameterTypes.remove(index);
        return this;
    }

    public int getParameterTypesSize() {
        return parameterTypes.size();
    }

    public List<Class<?>> getParameterTypes() {
        return parameterTypes;
    }

    public CallDefNode clearParameterTypes() {
        parameterTypes.clear();
        return this;
    }
    
    /* ---- end node data ---- */

    public CallDefNode() {
        // do nothing
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        for (ExpressionNode argumentNode : argumentNodes) {
            argumentNode.write(classWriter, methodWriter, globals);
        }

        // create method type from return value and arguments
        Type[] asmParameterTypes = new Type[parameterTypes.size()];
        for (int index = 0; index < asmParameterTypes.length; ++index) {
            asmParameterTypes[index] = Type.getType(parameterTypes.get(index));
        }
        Type methodType = Type.getMethodType(MethodWriter.getType(getType()), asmParameterTypes);

        List<Object> args = new ArrayList<>();
        args.add(recipe);
        args.addAll(pointers);
        methodWriter.invokeDefCall(name, methodType, DefBootstrap.METHOD_CALL, args.toArray());
    }
}
