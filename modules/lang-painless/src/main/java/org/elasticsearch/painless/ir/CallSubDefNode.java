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
import org.elasticsearch.painless.symbol.ScopeTable;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.List;

public class CallSubDefNode extends ArgumentsNode {

    /* ---- begin node data ---- */

    private String name;
    private String recipe;
    private final List<String> pointers = new ArrayList<>();
    private final List<Class<?>> typeParameters = new ArrayList<>();

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setRecipe(String recipe) {
        this.recipe = recipe;
    }

    public String getRecipe() {
        return recipe;
    }

    public void addPointer(String pointer) {
        pointers.add(pointer);
    }

    public List<String> getPointers() {
        return pointers;
    }

    public void addTypeParameter(Class<?> typeParameter) {
        typeParameters.add(typeParameter);
    }

    public List<Class<?>> getTypeParameters() {
        return typeParameters;
    }

    /* ---- end node data ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        methodWriter.writeDebugInfo(location);

        for (ExpressionNode argumentNode : getArgumentNodes()) {
            argumentNode.write(classWriter, methodWriter, globals, scopeTable);
        }

        // create method type from return value and arguments
        Type[] asmParameterTypes = new Type[typeParameters.size()];
        for (int index = 0; index < asmParameterTypes.length; ++index) {
            asmParameterTypes[index] = MethodWriter.getType(typeParameters.get(index));
        }
        Type methodType = Type.getMethodType(MethodWriter.getType(getExpressionType()), asmParameterTypes);

        List<Object> args = new ArrayList<>();
        args.add(recipe);
        args.addAll(pointers);
        methodWriter.invokeDefCall(name, methodType, DefBootstrap.METHOD_CALL, args.toArray());
    }
}
