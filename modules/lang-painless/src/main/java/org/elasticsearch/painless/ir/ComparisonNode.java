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
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScopeTable;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;

import static org.elasticsearch.painless.WriterConstants.EQUALS;
import static org.elasticsearch.painless.WriterConstants.OBJECTS_TYPE;

public class ComparisonNode extends BinaryNode {

    /* ---- begin node data ---- */

    private Operation operation;
    private Class<?> comparisonType;

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setComparisonType(Class<?> comparisonType) {
        this.comparisonType = comparisonType;
    }

    public Class<?> getComparisonType() {
        return comparisonType;
    }

    public String getComparisonCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(comparisonType);
    }

    /* ---- end node data ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        methodWriter.writeDebugInfo(location);

        getLeftNode().write(classWriter, methodWriter, globals, scopeTable);

        if (getRightNode() instanceof NullNode == false) {
            getRightNode().write(classWriter, methodWriter, globals, scopeTable);
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

        Type type = MethodWriter.getType(comparisonType);

        if (comparisonType == void.class || comparisonType == byte.class
                || comparisonType == short.class || comparisonType == char.class) {
            throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                    "for type [" + getExpressionCanonicalTypeName() + "]");
        } else if (comparisonType == boolean.class) {
            if (eq) methodWriter.ifCmp(type, MethodWriter.EQ, jump);
            else if (ne) methodWriter.ifCmp(type, MethodWriter.NE, jump);
            else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + getExpressionCanonicalTypeName() + "]");
            }
        } else if (comparisonType == int.class || comparisonType == long.class
                || comparisonType == float.class || comparisonType == double.class) {
            if (eq) methodWriter.ifCmp(type, MethodWriter.EQ, jump);
            else if (ne) methodWriter.ifCmp(type, MethodWriter.NE, jump);
            else if (lt) methodWriter.ifCmp(type, MethodWriter.LT, jump);
            else if (lte) methodWriter.ifCmp(type, MethodWriter.LE, jump);
            else if (gt) methodWriter.ifCmp(type, MethodWriter.GT, jump);
            else if (gte) methodWriter.ifCmp(type, MethodWriter.GE, jump);
            else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + getExpressionCanonicalTypeName() + "]");
            }

        } else if (comparisonType == def.class) {
            Type booleanType = Type.getType(boolean.class);
            Type descriptor = Type.getMethodType(booleanType,
                    MethodWriter.getType(getLeftNode().getExpressionType()), MethodWriter.getType(getRightNode().getExpressionType()));

            if (eq) {
                if (getRightNode() instanceof NullNode) {
                    methodWriter.ifNull(jump);
                } else if (getLeftNode() instanceof NullNode == false && operation == Operation.EQ) {
                    methodWriter.invokeDefCall("eq", descriptor, DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
                    writejump = false;
                } else {
                    methodWriter.ifCmp(type, MethodWriter.EQ, jump);
                }
            } else if (ne) {
                if (getRightNode() instanceof NullNode) {
                    methodWriter.ifNonNull(jump);
                } else if (getLeftNode() instanceof NullNode == false && operation == Operation.NE) {
                    methodWriter.invokeDefCall("eq", descriptor, DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
                    methodWriter.ifZCmp(MethodWriter.EQ, jump);
                } else {
                    methodWriter.ifCmp(type, MethodWriter.NE, jump);
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
                        "for type [" + getExpressionCanonicalTypeName() + "]");
            }
        } else {
            if (eq) {
                if (getRightNode() instanceof NullNode) {
                    methodWriter.ifNull(jump);
                } else if (operation == Operation.EQ) {
                    methodWriter.invokeStatic(OBJECTS_TYPE, EQUALS);
                    writejump = false;
                } else {
                    methodWriter.ifCmp(type, MethodWriter.EQ, jump);
                }
            } else if (ne) {
                if (getRightNode() instanceof NullNode) {
                    methodWriter.ifNonNull(jump);
                } else if (operation == Operation.NE) {
                    methodWriter.invokeStatic(OBJECTS_TYPE, EQUALS);
                    methodWriter.ifZCmp(MethodWriter.EQ, jump);
                } else {
                    methodWriter.ifCmp(type, MethodWriter.NE, jump);
                }
            } else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + getExpressionCanonicalTypeName() + "]");
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
