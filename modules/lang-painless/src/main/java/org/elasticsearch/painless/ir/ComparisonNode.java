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
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;

import static org.elasticsearch.painless.WriterConstants.EQUALS;
import static org.elasticsearch.painless.WriterConstants.OBJECTS_TYPE;

public class ComparisonNode extends BinaryNode {

    /* ---- begin tree structure ---- */

    @Override
    public ComparisonNode setLeftNode(ExpressionNode leftNode) {
        super.setLeftNode(leftNode);
        return this;
    }

    @Override
    public ComparisonNode setRightNode(ExpressionNode rightNode) {
        super.setRightNode(rightNode);
        return this;
    }

    @Override
    public ComparisonNode setTypeNode(TypeNode typeNode) {
        super.setTypeNode(typeNode);
        return this;
    }

    /* ---- end tree structure, begin node data ---- */

    protected Operation operation;

    public ComparisonNode setOperation(Operation operation) {
        this.operation = operation;
        return this;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    public ComparisonNode setLocation(Location location) {
        super.setLocation(location);
        return this;
    }

    /* ---- end node data ---- */

    public ComparisonNode() {
        // do nothing
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        leftNode.write(classWriter, methodWriter, globals);

        if (rightNode instanceof NullNode == false) {
            rightNode.write(classWriter, methodWriter, globals);
        }

        Label jump = new Label();
        Label end = new Label();

        boolean eq = (operation == Operation.EQ || operation == Operation.EQR);
        boolean ne = (operation == Operation.NE || operation == Operation.NER);
        boolean lt  = operation == Operation.LT;
        boolean lte = operation == Operation.LTE;
        boolean gt  = operation == Operation.GT;
        boolean gte = operation == Operation.GTE;

        boolean writejump = true;

        Class<?> type = getType();
        Type asmType = MethodWriter.getType(type);

        if (type == void.class || type == byte.class || type == short.class || type == char.class) {
            throw new IllegalStateException("unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for comparison");
        } else if (type == boolean.class) {
            if (eq) methodWriter.ifCmp(asmType, MethodWriter.EQ, jump);
            else if (ne) methodWriter.ifCmp(asmType, MethodWriter.NE, jump);
            else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + getCanonicalTypeName() + "]");
            }
        } else if (type == int.class || type == long.class || type == float.class || type == double.class) {
            if (eq) methodWriter.ifCmp(asmType, MethodWriter.EQ, jump);
            else if (ne) methodWriter.ifCmp(asmType, MethodWriter.NE, jump);
            else if (lt) methodWriter.ifCmp(asmType, MethodWriter.LT, jump);
            else if (lte) methodWriter.ifCmp(asmType, MethodWriter.LE, jump);
            else if (gt) methodWriter.ifCmp(asmType, MethodWriter.GT, jump);
            else if (gte) methodWriter.ifCmp(asmType, MethodWriter.GE, jump);
            else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + getCanonicalTypeName() + "]");
            }

        } else if (type == def.class) {
            Type booleanType = Type.getType(boolean.class);
            Type descriptor = Type.getMethodType(booleanType,
                    MethodWriter.getType(leftNode.getType()), MethodWriter.getType(rightNode.getType()));

            if (eq) {
                if (rightNode instanceof NullNode) {
                    methodWriter.ifNull(jump);
                } else if (leftNode instanceof NullNode == false && operation == Operation.EQ) {
                    methodWriter.invokeDefCall("eq", descriptor, DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
                    writejump = false;
                } else {
                    methodWriter.ifCmp(asmType, MethodWriter.EQ, jump);
                }
            } else if (ne) {
                if (rightNode instanceof NullNode) {
                    methodWriter.ifNonNull(jump);
                } else if (leftNode instanceof NullNode == false && operation == Operation.NE) {
                    methodWriter.invokeDefCall("eq", descriptor, DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
                    methodWriter.ifZCmp(MethodWriter.EQ, jump);
                } else {
                    methodWriter.ifCmp(asmType, MethodWriter.NE, jump);
                }
            } else if (lt) {
                methodWriter.invokeDefCall("lt", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else if (lte) {
                methodWriter.invokeDefCall("lte", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else if (gt) {
                methodWriter.invokeDefCall("gt", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else if (gte) {
                methodWriter.invokeDefCall("gte", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + getCanonicalTypeName() + "]");
            }
        } else {
            if (eq) {
                if (rightNode instanceof NullNode) {
                    methodWriter.ifNull(jump);
                } else if (operation == Operation.EQ) {
                    methodWriter.invokeStatic(OBJECTS_TYPE, EQUALS);
                    writejump = false;
                } else {
                    methodWriter.ifCmp(asmType, MethodWriter.EQ, jump);
                }
            } else if (ne) {
                if (rightNode instanceof NullNode) {
                    methodWriter.ifNonNull(jump);
                } else if (operation == Operation.NE) {
                    methodWriter.invokeStatic(OBJECTS_TYPE, EQUALS);
                    methodWriter.ifZCmp(MethodWriter.EQ, jump);
                } else {
                    methodWriter.ifCmp(asmType, MethodWriter.NE, jump);
                }
            } else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + getCanonicalTypeName() + "]");
            }
        }

        if (writejump) {
            methodWriter.push(false);
            methodWriter.goTo(end);
            methodWriter.mark(jump);
            methodWriter.push(true);
            methodWriter.mark(end);
        }
    }
}
