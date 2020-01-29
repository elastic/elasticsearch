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
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.symbol.ScopeTable;
import org.objectweb.asm.Type;

public class InstanceofNode extends UnaryNode {
    
    /* ---- begin node data ---- */

    private Class<?> instanceType;
    private Class<?> resolvedType;
    private boolean isPrimitiveResult;

    public void setInstanceType(Class<?> instanceType) {
        this.instanceType = instanceType;
    }
    
    public Class<?> getInstanceType() {
        return instanceType;
    }
    
    public String getInstanceCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(instanceType);
    }

    public void setResolvedType(Class<?> resolvedType) {
        this.resolvedType = resolvedType;
    }

    public Class<?> getResolvedType() {
        return resolvedType;
    }

    public String getResolvedCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(resolvedType);
    }
    
    public void setPrimitiveResult(boolean isPrimitiveResult) {
        this.isPrimitiveResult = isPrimitiveResult;
    }

    public boolean isPrimitiveResult() {
        return isPrimitiveResult;
    }

    /* ---- end node data ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        getChildNode().write(classWriter, methodWriter, globals, scopeTable);

        // primitive types
        if (isPrimitiveResult) {
            // discard child's result result
            methodWriter.writePop(MethodWriter.getType(getExpressionType()).getSize());
            // push our result: its' a primitive so it cannot be null
            methodWriter.push(resolvedType.isAssignableFrom(instanceType));
        } else {
            // ordinary instanceof
            methodWriter.instanceOf(Type.getType(resolvedType));
        }
    }
}
