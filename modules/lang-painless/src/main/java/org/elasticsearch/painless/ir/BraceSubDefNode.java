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

public class BraceSubDefNode extends UnaryNode {

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        setup(classWriter, methodWriter, globals, scopeTable);
        load(classWriter, methodWriter, globals, scopeTable);
    }

    @Override
    protected int accessElementCount() {
        return 2;
    }

    @Override
    protected void setup(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        methodWriter.dup();
        getChildNode().write(classWriter, methodWriter, globals, scopeTable);
        Type methodType = Type.getMethodType(MethodWriter.getType(
                getChildNode().getExpressionType()), Type.getType(Object.class), MethodWriter.getType(getChildNode().getExpressionType()));
        methodWriter.invokeDefCall("normalizeIndex", methodType, DefBootstrap.INDEX_NORMALIZE);
    }

    @Override
    protected void load(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        methodWriter.writeDebugInfo(location);

        Type methodType = Type.getMethodType(MethodWriter.getType(
                getExpressionType()), Type.getType(Object.class), MethodWriter.getType(getChildNode().getExpressionType()));
        methodWriter.invokeDefCall("arrayLoad", methodType, DefBootstrap.ARRAY_LOAD);
    }

    @Override
    protected void store(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        methodWriter.writeDebugInfo(location);

        Type methodType = Type.getMethodType(Type.getType(void.class), Type.getType(Object.class),
                MethodWriter.getType(getChildNode().getExpressionType()), MethodWriter.getType(getExpressionType()));
        methodWriter.invokeDefCall("arrayStore", methodType, DefBootstrap.ARRAY_STORE);
    }
}
