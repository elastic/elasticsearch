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
import org.objectweb.asm.Type;

public class InstanceofNode extends UnaryNode {

    protected final boolean isPrimitiveResult;

    public InstanceofNode(boolean isPrimitiveResult) {
        this.isPrimitiveResult = isPrimitiveResult;
    }

    protected TypeNode expressionTypeNode;
    protected TypeNode resolvedTypeNode;

    public void setExpressionTypeNode(TypeNode expressionTypeNode) {
        this.expressionTypeNode = expressionTypeNode;
    }

    public void setResolvedTypeNode(TypeNode resolvedTypeNode) {
        this.resolvedTypeNode = resolvedTypeNode;
    }

    public TypeNode setExpressionTypeNode() {
        return expressionTypeNode;
    }

    public TypeNode setResolvedTypeNode() {
        return resolvedTypeNode;
    }

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        childNode.write(classWriter, methodWriter, globals);

        // primitive types
        if (isPrimitiveResult) {
            // discard child's result result
            methodWriter.writePop(MethodWriter.getType(childNode.getType()).getSize());
            // push our result: its' a primitive so it cannot be null
            methodWriter.push(resolvedTypeNode.getType().isAssignableFrom(expressionTypeNode.getType()));
        } else {
            // ordinary instanceof
            methodWriter.instanceOf(Type.getType(resolvedTypeNode.getType()));
        }
    }
}
