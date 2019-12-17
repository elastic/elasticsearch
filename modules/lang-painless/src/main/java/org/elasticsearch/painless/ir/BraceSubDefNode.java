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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Type;

import java.util.Objects;

public class BraceSubDefNode extends UnaryNode {

    protected final Location location;

    public BraceSubDefNode(Location location) {
        this.location = Objects.requireNonNull(location);
    }

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        setup(classWriter, methodWriter, globals);
        load(classWriter, methodWriter, globals);
    }

    @Override
    public int accessElementCount() {
        return 2;
    }

    @Override
    public void setup(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.dup();
        childNode.write(classWriter, methodWriter, globals);
        Type methodType = Type.getMethodType(
                MethodWriter.getType(childNode.getType()), Type.getType(Object.class), MethodWriter.getType(childNode.getType()));
        methodWriter.invokeDefCall("normalizeIndex", methodType, DefBootstrap.INDEX_NORMALIZE);
    }

    @Override
    public void load(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        Type methodType =
                Type.getMethodType(MethodWriter.getType(getType()), Type.getType(Object.class), MethodWriter.getType(childNode.getType()));
        methodWriter.invokeDefCall("arrayLoad", methodType, DefBootstrap.ARRAY_LOAD);
    }

    @Override
    public void store(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        Type methodType = Type.getMethodType(Type.getType(void.class), Type.getType(Object.class),
                MethodWriter.getType(childNode.getType()), MethodWriter.getType(getType()));
        methodWriter.invokeDefCall("arrayStore", methodType, DefBootstrap.ARRAY_STORE);
    }
}
