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

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Variables;
import org.objectweb.asm.Label;
import org.elasticsearch.painless.MethodWriter;

import static org.elasticsearch.painless.WriterConstants.DEF_NEG_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_NOT_CALL;

/**
 * Represents a unary math expression.
 */
public final class EUnary extends AExpression {

    Operation operation;
    AExpression child;

    public EUnary(final int line, final String location, final Operation operation, final AExpression child) {
        super(line, location);

        this.operation = operation;
        this.child = child;
    }

    @Override
    void analyze(final CompilerSettings settings, final Variables variables) {
        if (operation == Operation.NOT) {
            analyzeNot(settings, variables);
        } else if (operation == Operation.BWNOT) {
            analyzeBWNot(settings, variables);
        } else if (operation == Operation.ADD) {
            analyzerAdd(settings, variables);
        } else if (operation == Operation.SUB) {
            analyzerSub(settings, variables);
        } else {
            throw new IllegalStateException(error("Illegal tree structure."));
        }
    }

    void analyzeNot(final CompilerSettings settings, final Variables variables) {
        child.expected = Definition.booleanType;
        child.analyze(settings, variables);
        child = child.cast(settings, variables);

        if (child.constant != null) {
            constant = !(boolean)child.constant;
        }

        actual = Definition.booleanType;
    }

    void analyzeBWNot(final CompilerSettings settings, final Variables variables) {
        child.analyze(settings, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(child.actual, false, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply not [~] to type [" + child.actual.name + "]."));
        }

        child.expected = promote;
        child = child.cast(settings, variables);

        if (child.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = ~(int)child.constant;
            } else if (sort == Sort.LONG) {
                constant = ~(long)child.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    void analyzerAdd(final CompilerSettings settings, final Variables variables) {
        child.analyze(settings, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(child.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply positive [+] to type [" + child.actual.name + "]."));
        }

        child.expected = promote;
        child = child.cast(settings, variables);

        if (child.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = +(int)child.constant;
            } else if (sort == Sort.LONG) {
                constant = +(long)child.constant;
            } else if (sort == Sort.FLOAT) {
                constant = +(float)child.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = +(double)child.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    void analyzerSub(final CompilerSettings settings, final Variables variables) {
        child.analyze(settings, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(child.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply negative [-] to type [" + child.actual.name + "]."));
        }

        child.expected = promote;
        child = child.cast(settings, variables);

        if (child.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = -(int)child.constant;
            } else if (sort == Sort.LONG) {
                constant = -(long)child.constant;
            } else if (sort == Sort.FLOAT) {
                constant = -(float)child.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = -(double)child.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    @Override
    void write(final CompilerSettings settings, final MethodWriter adapter) {
        if (operation == Operation.NOT) {
            if (tru == null && fals == null) {
                final Label localfals = new Label();
                final Label end = new Label();

                child.fals = localfals;
                child.write(settings, adapter);

                adapter.push(false);
                adapter.goTo(end);
                adapter.mark(localfals);
                adapter.push(true);
                adapter.mark(end);
            } else {
                child.tru = fals;
                child.fals = tru;
                child.write(settings, adapter);
            }
        } else {
            final org.objectweb.asm.Type type = actual.type;
            final Sort sort = actual.sort;

            child.write(settings, adapter);

            if (operation == Operation.BWNOT) {
                if (sort == Sort.DEF) {
                    adapter.invokeStatic(Definition.defobjType.type, DEF_NOT_CALL);
                } else {
                    if (sort == Sort.INT) {
                        adapter.push(-1);
                    } else if (sort == Sort.LONG) {
                        adapter.push(-1L);
                    } else {
                        throw new IllegalStateException(error("Illegal tree structure."));
                    }

                    adapter.math(MethodWriter.XOR, type);
                }
            } else if (operation == Operation.SUB) {
                if (sort == Sort.DEF) {
                    adapter.invokeStatic(Definition.defobjType.type, DEF_NEG_CALL);
                } else {
                    adapter.math(MethodWriter.NEG, type);
                }
            } else if (operation != Operation.ADD) {
                throw new IllegalStateException(error("Illegal tree structure."));
            }

            adapter.writeBranch(tru, fals);
        }
    }
}
