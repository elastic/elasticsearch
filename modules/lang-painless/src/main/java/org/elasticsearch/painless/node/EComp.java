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

import static org.elasticsearch.painless.WriterConstants.CHECKEQUALS;
import static org.elasticsearch.painless.WriterConstants.DEF_EQ_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_GTE_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_GT_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_LTE_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_LT_CALL;

/**
 * Represents a comparison expression.
 */
public final class EComp extends AExpression {

    final Operation operation;
    AExpression left;
    AExpression right;

    public EComp(final int line, final String location, final Operation operation, final AExpression left, final AExpression right) {
        super(line, location);

        this.operation = operation;
        this.left = left;
        this.right = right;
    }

    @Override
    void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        if (operation == Operation.EQ) {
            analyzeEq(settings, definition, variables);
        } else if (operation == Operation.EQR) {
            analyzeEqR(settings, definition, variables);
        } else if (operation == Operation.NE) {
            analyzeNE(settings, definition, variables);
        } else if (operation == Operation.NER) {
            analyzeNER(settings, definition, variables);
        } else if (operation == Operation.GTE) {
            analyzeGTE(settings, definition, variables);
        } else if (operation == Operation.GT) {
            analyzeGT(settings, definition, variables);
        } else if (operation == Operation.LTE) {
            analyzeLTE(settings, definition, variables);
        } else if (operation == Operation.LT) {
            analyzeLT(settings, definition, variables);
        } else {
            throw new IllegalStateException(error("Illegal tree structure."));
        }
    }

    private void analyzeEq(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteEquality(definition, left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply equals [==] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.isNull && right.isNull) {
            throw new IllegalArgumentException(error("Extraneous comparison of null constants."));
        }

        if ((left.constant != null || left.isNull) && (right.constant != null || right.isNull)) {
            final Sort sort = promote.sort;

            if (sort == Sort.BOOL) {
                constant = (boolean)left.constant == (boolean)right.constant;
            } else if (sort == Sort.INT) {
                constant = (int)left.constant == (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant == (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant == (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant == (double)right.constant;
            } else if (!left.isNull) {
                constant = left.constant.equals(right.constant);
            } else if (!right.isNull) {
                constant = right.constant.equals(null);
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = Definition.booleanType;
    }

    private void analyzeEqR(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteReference(definition, left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply reference equals [===] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.isNull && right.isNull) {
            throw new IllegalArgumentException(error("Extraneous comparison of null constants."));
        }

        if ((left.constant != null || left.isNull) && (right.constant != null || right.isNull)) {
            final Sort sort = promote.sort;

            if (sort == Sort.BOOL) {
                constant = (boolean)left.constant == (boolean)right.constant;
            } else if (sort == Sort.INT) {
                constant = (int)left.constant == (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant == (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant == (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant == (double)right.constant;
            } else {
                constant = left.constant == right.constant;
            }
        }

        actual = Definition.booleanType;
    }

    private void analyzeNE(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteEquality(definition, left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply not equals [!=] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.isNull && right.isNull) {
            throw new IllegalArgumentException(error("Extraneous comparison of null constants."));
        }

        if ((left.constant != null || left.isNull) && (right.constant != null || right.isNull)) {
            final Sort sort = promote.sort;

            if (sort == Sort.BOOL) {
                constant = (boolean)left.constant != (boolean)right.constant;
            } else if (sort == Sort.INT) {
                constant = (int)left.constant != (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant != (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant != (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant != (double)right.constant;
            } else if (!left.isNull) {
                constant = !left.constant.equals(right.constant);
            } else if (!right.isNull) {
                constant = !right.constant.equals(null);
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = Definition.booleanType;
    }

    private void analyzeNER(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteReference(definition, left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply reference not equals [!==] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.isNull && right.isNull) {
            throw new IllegalArgumentException(error("Extraneous comparison of null constants."));
        }

        if ((left.constant != null || left.isNull) && (right.constant != null || right.isNull)) {
            final Sort sort = promote.sort;

            if (sort == Sort.BOOL) {
                constant = (boolean)left.constant != (boolean)right.constant;
            } else if (sort == Sort.INT) {
                constant = (int)left.constant != (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant != (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant != (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant != (double)right.constant;
            } else {
                constant = left.constant != right.constant;
            }
        }

        actual = Definition.booleanType;
    }

    private void analyzeGTE(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, right.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply greater than or equals [>=] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant >= (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant >= (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant >= (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant >= (double)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = Definition.booleanType;
    }

    private void analyzeGT(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, right.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply greater than [>] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant > (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant > (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant > (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant > (double)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = Definition.booleanType;
    }

    private void analyzeLTE(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, right.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply less than or equals [<=] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant <= (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant <= (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant <= (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant <= (double)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = Definition.booleanType;
    }

    private void analyzeLT(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, right.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply less than [>=] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant < (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant < (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant < (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant < (double)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = Definition.booleanType;
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        final boolean branch = tru != null || fals != null;
        final org.objectweb.asm.Type rtype = right.actual.type;
        final Sort rsort = right.actual.sort;

        left.write(settings, definition, adapter);

        if (!right.isNull) {
            right.write(settings, definition, adapter);
        }

        final Label jump = tru != null ? tru : fals != null ? fals : new Label();
        final Label end = new Label();

        final boolean eq = (operation == Operation.EQ || operation == Operation.EQR) && (tru != null || fals == null) ||
            (operation == Operation.NE || operation == Operation.NER) && fals != null;
        final boolean ne = (operation == Operation.NE || operation == Operation.NER) && (tru != null || fals == null) ||
            (operation == Operation.EQ || operation == Operation.EQR) && fals != null;
        final boolean lt  = operation == Operation.LT  && (tru != null || fals == null) || operation == Operation.GTE && fals != null;
        final boolean lte = operation == Operation.LTE && (tru != null || fals == null) || operation == Operation.GT  && fals != null;
        final boolean gt  = operation == Operation.GT  && (tru != null || fals == null) || operation == Operation.LTE && fals != null;
        final boolean gte = operation == Operation.GTE && (tru != null || fals == null) || operation == Operation.LT  && fals != null;

        boolean writejump = true;

        switch (rsort) {
            case VOID:
            case BYTE:
            case SHORT:
            case CHAR:
                throw new IllegalStateException(error("Illegal tree structure."));
            case BOOL:
                if      (eq) adapter.ifZCmp(MethodWriter.EQ, jump);
                else if (ne) adapter.ifZCmp(MethodWriter.NE, jump);
                else {
                    throw new IllegalStateException(error("Illegal tree structure."));
                }

                break;
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                if      (eq)  adapter.ifCmp(rtype, MethodWriter.EQ, jump);
                else if (ne)  adapter.ifCmp(rtype, MethodWriter.NE, jump);
                else if (lt)  adapter.ifCmp(rtype, MethodWriter.LT, jump);
                else if (lte) adapter.ifCmp(rtype, MethodWriter.LE, jump);
                else if (gt)  adapter.ifCmp(rtype, MethodWriter.GT, jump);
                else if (gte) adapter.ifCmp(rtype, MethodWriter.GE, jump);
                else {
                    throw new IllegalStateException(error("Illegal tree structure."));
                }

                break;
            case DEF:
                if (eq) {
                    if (right.isNull) {
                        adapter.ifNull(jump);
                    } else if (!left.isNull && operation == Operation.EQ) {
                        adapter.invokeStatic(Definition.defobjType.type, DEF_EQ_CALL);
                    } else {
                        adapter.ifCmp(rtype, MethodWriter.EQ, jump);
                    }
                } else if (ne) {
                    if (right.isNull) {
                        adapter.ifNonNull(jump);
                    } else if (!left.isNull && operation == Operation.NE) {
                        adapter.invokeStatic(Definition.defobjType.type, DEF_EQ_CALL);
                        adapter.ifZCmp(MethodWriter.EQ, jump);
                    } else {
                        adapter.ifCmp(rtype, MethodWriter.NE, jump);
                    }
                } else if (lt) {
                    adapter.invokeStatic(Definition.defobjType.type, DEF_LT_CALL);
                } else if (lte) {
                    adapter.invokeStatic(Definition.defobjType.type, DEF_LTE_CALL);
                } else if (gt) {
                    adapter.invokeStatic(Definition.defobjType.type, DEF_GT_CALL);
                } else if (gte) {
                    adapter.invokeStatic(Definition.defobjType.type, DEF_GTE_CALL);
                } else {
                    throw new IllegalStateException(error("Illegal tree structure."));
                }

                writejump = left.isNull || ne || operation == Operation.EQR;

                if (branch && !writejump) {
                    adapter.ifZCmp(MethodWriter.NE, jump);
                }

                break;
            default:
                if (eq) {
                    if (right.isNull) {
                        adapter.ifNull(jump);
                    } else if (operation == Operation.EQ) {
                        adapter.invokeStatic(definition.getType("Utility").type, CHECKEQUALS);

                        if (branch) {
                            adapter.ifZCmp(MethodWriter.NE, jump);
                        }

                        writejump = false;
                    } else {
                        adapter.ifCmp(rtype, MethodWriter.EQ, jump);
                    }
                } else if (ne) {
                    if (right.isNull) {
                        adapter.ifNonNull(jump);
                    } else if (operation == Operation.NE) {
                        adapter.invokeStatic(definition.getType("Utility").type, CHECKEQUALS);
                        adapter.ifZCmp(MethodWriter.EQ, jump);
                    } else {
                        adapter.ifCmp(rtype, MethodWriter.NE, jump);
                    }
                } else {
                    throw new IllegalStateException(error("Illegal tree structure."));
                }
        }

        if (!branch && writejump) {
            adapter.push(false);
            adapter.goTo(end);
            adapter.mark(jump);
            adapter.push(true);
            adapter.mark(end);
        }
    }
}
