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

public class FieldNode extends IRNode {

    /* ---- begin node data ---- */

    private int modifiers;
    private Class<?> fieldType;
    private String name;
    private Object instance;

    public void setModifiers(int modifiers) {
        this.modifiers = modifiers;
    }

    public int getModifiers(int modifiers) {
        return modifiers;
    }

    public void setFieldType(Class<?> fieldType) {
        this.fieldType = fieldType;
    }

    public Class<?> getFieldType() {
        return fieldType;
    }

    public String getFieldCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(fieldType);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setInstance(Object instance) {
        this.instance = instance;
    }

    public Object getInstance() {
        return instance;
    }

    /* ---- end node data ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        classWriter.getClassVisitor().visitField(
                ClassWriter.buildAccess(modifiers, true), name, Type.getType(fieldType).getDescriptor(), null, null).visitEnd();
    }
}
