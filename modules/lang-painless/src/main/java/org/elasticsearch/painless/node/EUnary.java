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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Locals;
import org.objectweb.asm.Label;
import org.elasticsearch.painless.MethodWriter;

import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_HANDLE;

/**
 * Represents a unary math expression.
 */
public final class EUnary extends AExpression {

    final Operation operation;
    AExpression child;
    Type promote;

    public EUnary(Location location, Operation operation, AExpression child) {
        super(location);

        this.operation = operation;
        this.child = child;
    }

    @Override
    void analyze(Locals locals) {
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
        child.expected = Definition.BOOLEAN_TYPE;
        child.analyze(variables);
        child = child.cast(variables);

        if (child.constant != null) {
            constant = !(boolean)child.constant;
        }

        actual = Definition.BOOLEAN_TYPE;
    }

    void analyzeBWNot(Locals variables) {
        child.analyze(variables);

        promote = AnalyzerCaster.promoteNumeric(child.actual, false);

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply not [~] to type [" + child.actual.name + "]."));
        }

        child.expected = promote;
        child = child.cast(variables);

        if (child.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = ~(int)child.constant;
            } else if (sort == Sort.LONG) {
                constant = ~(long)child.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        if (promote.sort == Sort.DEF && expected != null) {
            actual = expected;
        } else {
            actual = promote;
        }
    }

    void analyzerAdd(Locals variables) {
        child.analyze(variables);

        promote = AnalyzerCaster.promoteNumeric(child.actual, true);

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply positive [+] to type [" + child.actual.name + "]."));
        }

        child.expected = promote;
        child = child.cast(variables);

        if (child.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = +(int)child.constant;
            } else if (sort == Sort.LONG) {
                constant = +(long)child.constant;
            } else if (sort == Sort.FLOAT) {
                constant = +(float)child.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = +(double)child.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        if (promote.sort == Sort.DEF && expected != null) {
            actual = expected;
        } else {
            actual = promote;
        }
    }

    void analyzerSub(Locals variables) {
        child.analyze(variables);

        promote = AnalyzerCaster.promoteNumeric(child.actual, true);

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply negative [-] to type [" + child.actual.name + "]."));
        }

        child.expected = promote;
        child = child.cast(variables);

        if (child.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = -(int)child.constant;
            } else if (sort == Sort.LONG) {
                constant = -(long)child.constant;
            } else if (sort == Sort.FLOAT) {
                constant = -(float)child.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = -(double)child.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        if (promote.sort == Sort.DEF && expected != null) {
            actual = expected;
        } else {
            actual = promote;
        }
    }

    @Override
    void write(MethodWriter writer) {
        writer.writeDebugInfo(location);

        if (operation == Operation.NOT) {
            if (tru == null && fals == null) {
                Label localfals = new Label();
                Label end = new Label();

                child.fals = localfals;
                child.write(writer);

                writer.push(false);
                writer.goTo(end);
                writer.mark(localfals);
                writer.push(true);
                writer.mark(end);
            } else {
                child.tru = fals;
                child.fals = tru;
                child.write(writer);
            }
        } else {
            Sort sort = promote.sort;
            child.write(writer);

            if (operation == Operation.BWNOT) {
                if (sort == Sort.DEF) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actual.type, child.actual.type);
                    writer.invokeDynamic("not", descriptor.getDescriptor(), DEF_BOOTSTRAP_HANDLE, DefBootstrap.UNARY_OPERATOR, 0);
                } else {
                    if (sort == Sort.INT) {
                        writer.push(-1);
                    } else if (sort == Sort.LONG) {
                        writer.push(-1L);
                    } else {
                        throw createError(new IllegalStateException("Illegal tree structure."));
                    }

                    writer.math(MethodWriter.XOR, actual.type);
                }
            } else if (operation == Operation.SUB) {
                if (sort == Sort.DEF) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actual.type, child.actual.type);
                    writer.invokeDynamic("neg", descriptor.getDescriptor(), DEF_BOOTSTRAP_HANDLE, DefBootstrap.UNARY_OPERATOR, 0);
                } else {
                    writer.math(MethodWriter.NEG, actual.type);
                }
            } else if (operation == Operation.ADD) {
                if (sort == Sort.DEF) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actual.type, child.actual.type);
                    writer.invokeDynamic("plus", descriptor.getDescriptor(), DEF_BOOTSTRAP_HANDLE, DefBootstrap.UNARY_OPERATOR, 0);
                } 
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

            writer.writeBranch(tru, fals);
        }
    }
}
