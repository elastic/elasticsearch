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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DefCallNode extends ArgumentsNode {

    protected final Location location;
    protected final String name;
    protected final String recipe;
    protected final List<String> pointers;
    protected final List<Class<?>> parameterTypes;

    public DefCallNode(Location location, String name, String recipe, List<String> pointers, List<Class<?>> parameterTypes) {
        this.location = location;
        this.name = Objects.requireNonNull(name);
        this.recipe = Objects.requireNonNull(recipe);
        this.pointers = Collections.unmodifiableList(Objects.requireNonNull(pointers));
        this.parameterTypes = Collections.unmodifiableList(Objects.requireNonNull(parameterTypes));
    }

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        for (ExpressionNode argumentNode : argumentNodes) {
            argumentNode.write(classWriter, methodWriter, globals);
        }

        // create method type from return value and arguments
        Type[] asmParameterTypes = new Type[parameterTypes.size()];
        for (int index = 0; index < asmParameterTypes.length; ++index) {
            asmParameterTypes[index] = Type.getType(parameterTypes.get(index));
        }
        Type methodType = Type.getMethodType(MethodWriter.getType(getType()), asmParameterTypes);

        List<Object> args = new ArrayList<>();
        args.add(recipe);
        args.addAll(pointers);
        methodWriter.invokeDefCall(name, methodType, DefBootstrap.METHOD_CALL, args.toArray());
    }
}
