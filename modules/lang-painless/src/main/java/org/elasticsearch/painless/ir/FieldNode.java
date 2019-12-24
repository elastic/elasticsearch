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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Type;

public class FieldNode extends IRNode {

    /* ---- begin tree structure ---- */

    protected TypeNode typeNode;

    public void setTypeNode(TypeNode typeNode) {
        this.typeNode = typeNode;
    }

    public TypeNode getTypeNode() {
        return typeNode;
    }

    public Class<?> getType() {
        return typeNode.getType();
    }

    public String getCanonicalTypeName() {
        return typeNode.getCanonicalTypeName();
    }

    /* ---- end tree structure, begin node data ---- */

    protected int modifiers;
    protected String name;
    protected Object instance;

    public FieldNode setModifiers(int modifiers) {
        this.modifiers = modifiers;
        return this;
    }

    public int getModifiers(int modifiers) {
        return modifiers;
    }

    public FieldNode setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public FieldNode setInstance(Object instance) {
        this.instance = instance;
        return this;
    }

    public Object getInstance() {
        return instance;
    }

    @Override
    public FieldNode setLocation(Location location) {
        super.setLocation(location);
        return this;
    }

    /* ---- end node data ---- */

    public FieldNode() {
        // do nothing
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        classWriter.getClassVisitor().visitField(
                ClassWriter.buildAccess(modifiers, true), name, Type.getType(getType()).getDescriptor(), null, null).visitEnd();
    }
}
