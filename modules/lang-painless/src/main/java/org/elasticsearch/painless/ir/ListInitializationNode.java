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
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.util.Collection;

public class ListInitializationNode extends ArgumentsNode {

    /* ---- begin tree structure ---- */

    @Override
    public ListInitializationNode addArgumentNode(ExpressionNode argumentNode) {
        super.addArgumentNode(argumentNode);
        return this;
    }

    @Override
    public ListInitializationNode addArgumentNodes(Collection<ExpressionNode> argumentNodes) {
        super.addArgumentNodes(argumentNodes);
        return this;
    }

    @Override
    public ListInitializationNode setArgumentNode(int index, ExpressionNode argumentNode) {
        super.setArgumentNode(index, argumentNode);
        return this;
    }

    @Override
    public ListInitializationNode removeArgumentNode(ExpressionNode argumentNode) {
        super.removeArgumentNode(argumentNode);
        return this;
    }

    @Override
    public ListInitializationNode removeArgumentNode(int index) {
        super.removeArgumentNode(index);
        return this;
    }

    @Override
    public ListInitializationNode clearArgumentNodes() {
        super.clearArgumentNodes();
        return this;
    }

    @Override
    public ListInitializationNode setTypeNode(TypeNode typeNode) {
        super.setTypeNode(typeNode);
        return this;
    }

    /* ---- end tree structure, begin node data ---- */

    protected PainlessConstructor constructor;
    protected PainlessMethod method;

    public ListInitializationNode setConstructor(PainlessConstructor constructor) {
        this.constructor = constructor;
        return this;
    }

    public PainlessConstructor getConstructor() {
        return constructor;
    }

    public ListInitializationNode setMethod(PainlessMethod method) {
        this.method = method;
        return this;
    }

    public PainlessMethod getMethod() {
        return method;
    }

    @Override
    public ListInitializationNode setLocation(Location location) {
        super.setLocation(location);
        return this;
    }

    /* ---- end node data ---- */

    public ListInitializationNode() {
        // do nothing
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        methodWriter.newInstance(MethodWriter.getType(getType()));
        methodWriter.dup();
        methodWriter.invokeConstructor(
                    Type.getType(constructor.javaConstructor.getDeclaringClass()), Method.getMethod(constructor.javaConstructor));

        for (ExpressionNode argument : argumentNodes) {
            methodWriter.dup();
            argument.write(classWriter, methodWriter, globals);
            methodWriter.invokeMethodCall(method);
            methodWriter.pop();
        }
    }
}
