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

public class DotSubDefNode extends ExpressionNode {

    /* ---- begin node data ---- */

    private String value;

    public void setValue(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /* ---- end node data ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        methodWriter.writeDebugInfo(location);

        Type methodType = Type.getMethodType(MethodWriter.getType(getExpressionType()), Type.getType(Object.class));
        methodWriter.invokeDefCall(value, methodType, DefBootstrap.LOAD);
    }

    @Override
    protected int accessElementCount() {
        return 1;
    }

    @Override
    protected void setup(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        // do nothing
    }

    @Override
    protected void load(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        methodWriter.writeDebugInfo(location);

        Type methodType = Type.getMethodType(MethodWriter.getType(getExpressionType()), Type.getType(Object.class));
        methodWriter.invokeDefCall(value, methodType, DefBootstrap.LOAD);
    }

    @Override
    protected void store(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        methodWriter.writeDebugInfo(location);

        Type methodType = Type.getMethodType(
                Type.getType(void.class), Type.getType(Object.class), MethodWriter.getType(getExpressionType()));
        methodWriter.invokeDefCall(value, methodType, DefBootstrap.STORE);
    }
}
