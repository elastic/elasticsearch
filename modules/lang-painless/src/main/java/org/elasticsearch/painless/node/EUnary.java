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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Locals;
import org.objectweb.asm.Label;

import java.util.Objects;
import java.util.Set;

import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Opcodes;

/**
 * Represents a unary math expression.
 */
public final class EUnary extends AExpression {

    private final Operation operation;
    private AExpression child;

    private Type promote;
    private boolean originallyExplicit = false; // record whether there was originally an explicit cast

    public EUnary(Location location, Operation operation, AExpression child) {
        super(location);

        this.operation = Objects.requireNonNull(operation);
        this.child = Objects.requireNonNull(child);
    }

    @Override
    void extractVariables(Set<String> variables) {
        child.extractVariables(variables);
    }

    @Override
    void analyze(Locals locals) {
        originallyExplicit = explicit;

        if (operation == Operation.NOT) {
            analyzeNot(locals);
        } else if (operation == Operation.BWNOT) {
            analyzeBWNot(locals);
        } else if (operation == Operation.ADD) {
            analyzerAdd(locals);
        } else if (operation == Operation.SUB) {
            analyzerSub(locals);
        } else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    void analyzeNot(Locals variables) {
        child.expected = variables.getDefinition().booleanType;
        child.analyze(variables);
        child = child.cast(variables);

        if (child.constant != null) {
            constant = !(boolean)child.constant;
        }

        actual = variables.getDefinition().booleanType;
    }

    void analyzeBWNot(Locals variables) {
        child.analyze(variables);

        promote = variables.getDefinition().caster.promoteNumeric(child.actual, false);

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply not [~] to type [" + child.actual.name + "]."));
        }

        child.expected = promote;
        child = child.cast(variables);

        if (child.constant != null) {
            Class<?> sort = promote.clazz;

            if (sort == int.class) {
                constant = ~(int)child.constant;
            } else if (sort == long.class) {
                constant = ~(long)child.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        if (promote.dynamic && expected != null) {
            actual = expected;
        } else {
            actual = promote;
        }
    }

    void analyzerAdd(Locals variables) {
        child.analyze(variables);

        promote = variables.getDefinition().caster.promoteNumeric(child.actual, true);

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply positive [+] to type [" + child.actual.name + "]."));
        }

        child.expected = promote;
        child = child.cast(variables);

        if (child.constant != null) {
            Class<?> sort = promote.clazz;

            if (sort == int.class) {
                constant = +(int)child.constant;
            } else if (sort == long.class) {
                constant = +(long)child.constant;
            } else if (sort == float.class) {
                constant = +(float)child.constant;
            } else if (sort == double.class) {
                constant = +(double)child.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        if (promote.dynamic && expected != null) {
            actual = expected;
        } else {
            actual = promote;
        }
    }

    void analyzerSub(Locals variables) {
        child.analyze(variables);

        promote = variables.getDefinition().caster.promoteNumeric(child.actual, true);

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply negative [-] to type [" + child.actual.name + "]."));
        }

        child.expected = promote;
        child = child.cast(variables);

        if (child.constant != null) {
            Class<?> sort = promote.clazz;

            if (sort == int.class) {
                constant = -(int)child.constant;
            } else if (sort == long.class) {
                constant = -(long)child.constant;
            } else if (sort == float.class) {
                constant = -(float)child.constant;
            } else if (sort == double.class) {
                constant = -(double)child.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        if (promote.dynamic && expected != null) {
            actual = expected;
        } else {
            actual = promote;
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        if (operation == Operation.NOT) {
            Label fals = new Label();
            Label end = new Label();

            child.write(writer, globals);
            writer.ifZCmp(Opcodes.IFEQ, fals);

            writer.push(false);
            writer.goTo(end);
            writer.mark(fals);
            writer.push(true);
            writer.mark(end);
        } else {
            Class<?> sort = promote.clazz;
            child.write(writer, globals);

            // Def calls adopt the wanted return value. If there was a narrowing cast,
            // we need to flag that so that it's done at runtime.
            int defFlags = 0;

            if (originallyExplicit) {
                defFlags |= DefBootstrap.OPERATOR_EXPLICIT_CAST;
            }

            if (operation == Operation.BWNOT) {
                if (promote.dynamic) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actual.type, child.actual.type);
                    writer.invokeDefCall("not", descriptor, DefBootstrap.UNARY_OPERATOR, defFlags);
                } else {
                    if (sort == int.class) {
                        writer.push(-1);
                    } else if (sort == long.class) {
                        writer.push(-1L);
                    } else {
                        throw createError(new IllegalStateException("Illegal tree structure."));
                    }

                    writer.math(MethodWriter.XOR, actual.type);
                }
            } else if (operation == Operation.SUB) {
                if (promote.dynamic) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actual.type, child.actual.type);
                    writer.invokeDefCall("neg", descriptor, DefBootstrap.UNARY_OPERATOR, defFlags);
                } else {
                    writer.math(MethodWriter.NEG, actual.type);
                }
            } else if (operation == Operation.ADD) {
                if (promote.dynamic) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actual.type, child.actual.type);
                    writer.invokeDefCall("plus", descriptor, DefBootstrap.UNARY_OPERATOR, defFlags);
                }
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }
    }

    @Override
    public String toString() {
        return singleLineToString(operation.symbol, child);
    }
}
