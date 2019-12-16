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
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.util.Objects;

public final class NewObjectNode extends ArgumentsNode {

    protected final Location location;
    protected final PainlessConstructor constructor;
    protected final boolean read;

    public NewObjectNode(Location location, PainlessConstructor constructor, boolean read) {
        this.location = Objects.requireNonNull(location);
        this.constructor = Objects.requireNonNull(constructor);
        this.read = read;
    }

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        methodWriter.newInstance(MethodWriter.getType(getType()));

        if (read) {
            methodWriter.dup();
        }

        for (ExpressionNode argumentNode : argumentNodes) {
            argumentNode.write(classWriter, methodWriter, globals);
        }

        methodWriter.invokeConstructor(
                    Type.getType(constructor.javaConstructor.getDeclaringClass()), Method.getMethod(constructor.javaConstructor));
    }
}
