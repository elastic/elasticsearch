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
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.WriteScope;

public class LoadMapShortcutNode extends ExpressionNode {

    /* ---- begin node data ---- */

    private PainlessMethod getter;

    public void setGetter(PainlessMethod getter) {
        this.getter = getter;
    }

    public PainlessMethod getGetter() {
        return getter;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitLoadMapShortcut(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        // do nothing; terminal node
    }

    /* ---- end visitor ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        methodWriter.writeDebugInfo(location);
        methodWriter.invokeMethodCall(getter);

        if (getter.returnType != getter.javaMethod.getReturnType()) {
            methodWriter.checkCast(MethodWriter.getType(getter.returnType));
        }
    }
}
