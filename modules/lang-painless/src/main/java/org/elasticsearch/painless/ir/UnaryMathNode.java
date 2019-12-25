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
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.lookup.def;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class UnaryMathNode extends UnaryNode {

    /* ---- begin tree structure ---- */

    protected TypeNode unaryTypeNode;

    public UnaryMathNode setUnaryTypeNode(TypeNode unaryTypeNode) {
        this.unaryTypeNode = unaryTypeNode;
        return this;
    }

    public TypeNode getUnaryTypeNode() {
        return unaryTypeNode;
    }

    public Class<?> getUnaryType() {
        return unaryTypeNode.getType();
    }

    public String getUnaryCanonicalTypeName() {
        return unaryTypeNode.getCanonicalTypeName();
    }
    
    @Override
    public UnaryMathNode setTypeNode(TypeNode typeNode) {
        super.setTypeNode(typeNode);
        return this;
    }

    @Override
    public UnaryMathNode setChildNode(ExpressionNode childNode) {
        super.setChildNode(childNode);
        return this;
    }

    /* ---- end tree structure, begin node data ---- */

    protected Operation operation;
    protected boolean cat;
    protected boolean originallyExplicit; // record whether there was originally an explicit cast

    public UnaryMathNode setOperation(Operation operation) {
        this.operation = operation;
        return this;
    }

    public Operation getOperation() {
        return operation;
    }

    public UnaryMathNode setCat(boolean cat) {
        this.cat = cat;
        return this;
    }

    public boolean getCat() {
        return cat;
    }

    public UnaryMathNode setOriginallExplicit(boolean originallyExplicit) {
        this.originallyExplicit = originallyExplicit;
        return this;
    }

    public boolean getOriginallyExplicit() {
        return originallyExplicit;
    }

    @Override
    public UnaryMathNode setLocation(Location location) {
        super.setLocation(location);
        return this;
    }

    /* ---- end node data ---- */

    public UnaryMathNode() {
        // do nothing
    }

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        if (operation == Operation.NOT) {
            Label fals = new Label();
            Label end = new Label();

            childNode.write(classWriter, methodWriter, globals);
            methodWriter.ifZCmp(Opcodes.IFEQ, fals);

            methodWriter.push(false);
            methodWriter.goTo(end);
            methodWriter.mark(fals);
            methodWriter.push(true);
            methodWriter.mark(end);
        } else {
            childNode.write(classWriter, methodWriter, globals);

            // Def calls adopt the wanted return value. If there was a narrowing cast,
            // we need to flag that so that it's done at runtime.
            int defFlags = 0;

            if (originallyExplicit) {
                defFlags |= DefBootstrap.OPERATOR_EXPLICIT_CAST;
            }

            Type actualType = MethodWriter.getType(getType());
            Type childType = MethodWriter.getType(childNode.getType());

            if (operation == Operation.BWNOT) {
                if (getUnaryType() == def.class) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actualType, childType);
                    methodWriter.invokeDefCall("not", descriptor, DefBootstrap.UNARY_OPERATOR, defFlags);
                } else {
                    if (getUnaryType() == int.class) {
                        methodWriter.push(-1);
                    } else if (getUnaryType() == long.class) {
                        methodWriter.push(-1L);
                    } else {
                        throw new IllegalStateException("unexpected unary math operation [" + operation + "] " +
                                "for type [" + getCanonicalTypeName() + "]");
                    }

                    methodWriter.math(MethodWriter.XOR, actualType);
                }
            } else if (operation == Operation.SUB) {
                if (getUnaryType() == def.class) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actualType, childType);
                    methodWriter.invokeDefCall("neg", descriptor, DefBootstrap.UNARY_OPERATOR, defFlags);
                } else {
                    methodWriter.math(MethodWriter.NEG, actualType);
                }
            } else if (operation == Operation.ADD) {
                if (getUnaryType() == def.class) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actualType, childType);
                    methodWriter.invokeDefCall("plus", descriptor, DefBootstrap.UNARY_OPERATOR, defFlags);
                }
            } else {
                throw new IllegalStateException("unexpected unary math operation [" + operation + "] " +
                        "for type [" + getCanonicalTypeName() + "]");
            }
        }
    }
}
