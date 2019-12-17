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

import java.util.Objects;

public class DotSubDefNode extends ExpressionNode {

    protected final Location location;
    protected final String value;

    public DotSubDefNode(Location location, String value) {
        this.location = Objects.requireNonNull(location);
        this.value = Objects.requireNonNull(value);
    }

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        org.objectweb.asm.Type methodType =
            org.objectweb.asm.Type.getMethodType(MethodWriter.getType(getType()), org.objectweb.asm.Type.getType(Object.class));
        methodWriter.invokeDefCall(value, methodType, DefBootstrap.LOAD);
    }

    @Override
    public int accessElementCount() {
        return 1;
    }

    @Override
    public void setup(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        // do nothing
    }

    @Override
    public void load(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        org.objectweb.asm.Type methodType =
            org.objectweb.asm.Type.getMethodType(MethodWriter.getType(getType()), org.objectweb.asm.Type.getType(Object.class));
        methodWriter.invokeDefCall(value, methodType, DefBootstrap.LOAD);
    }

    @Override
    public void store(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        org.objectweb.asm.Type methodType = org.objectweb.asm.Type.getMethodType(
            org.objectweb.asm.Type.getType(void.class), org.objectweb.asm.Type.getType(Object.class), MethodWriter.getType(getType()));
        methodWriter.invokeDefCall(value, methodType, DefBootstrap.STORE);
    }
}
