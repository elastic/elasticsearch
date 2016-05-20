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

import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Variables;

/**
 * Represents a binary math expression.
 */
public final class EBinary extends AExpression {

    final Operation operation;
    AExpression left;
    AExpression right;

    boolean cat = false;

    public EBinary(final int line, final String location, final Operation operation, final AExpression left, final AExpression right) {
        super(line, location);

        this.operation = operation;
        this.left = left;
        this.right = right;
    }

    @Override
    void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        if (operation == Operation.MUL) {
            analyzeMul(settings, definition, variables);
        } else if (operation == Operation.DIV) {
            analyzeDiv(settings, definition, variables);
        } else if (operation == Operation.REM) {
            analyzeRem(settings, definition, variables);
        } else if (operation == Operation.ADD) {
            analyzeAdd(settings, definition, variables);
        } else if (operation == Operation.SUB) {
            analyzeSub(settings, definition, variables);
        } else if (operation == Operation.LSH) {
            analyzeLSH(settings, definition, variables);
        } else if (operation == Operation.RSH) {
            analyzeRSH(settings, definition, variables);
        } else if (operation == Operation.USH) {
            analyzeUSH(settings, definition, variables);
        } else if (operation == Operation.BWAND) {
            analyzeBWAnd(settings, definition, variables);
        } else if (operation == Operation.XOR) {
            analyzeXor(settings, definition, variables);
        } else if (operation == Operation.BWOR) {
            analyzeBWOr(settings, definition, variables);
        } else {
            throw new IllegalStateException(error("Illegal tree structure."));
        }
    }

    private void analyzeMul(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, right.actual, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply multiply [*] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant * (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant * (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant * (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant * (double)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeDiv(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, right.actual, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply divide [/] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant / (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant / (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant / (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant / (double)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeRem(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, right.actual, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply remainder [%] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant % (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant % (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant % (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant % (double)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeAdd(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteAdd(definition, left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply add [+] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        final Sort sort = promote.sort;

        if (sort == Sort.STRING) {
            left.expected = left.actual;

            if (left instanceof EBinary && ((EBinary)left).operation == Operation.ADD && left.actual.sort == Sort.STRING) {
                ((EBinary)left).cat = true;
            }

            right.expected = right.actual;

            if (right instanceof EBinary && ((EBinary)right).operation == Operation.ADD && right.actual.sort == Sort.STRING) {
                ((EBinary)right).cat = true;
            }
        } else {
            left.expected = promote;
            right.expected = promote;
        }

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            if (sort == Sort.INT) {
                constant = (int)left.constant + (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant + (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant + (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant + (double)right.constant;
            } else if (sort == Sort.STRING) {
                constant = "" + left.constant + right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeSub(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, right.actual, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply subtract [-] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant - (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant - (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant - (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant - (double)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeLSH(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, false);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply left shift [<<] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = definition.getType("int");
        right.explicit = true;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant << (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant << (int)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeRSH(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, false);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply right shift [>>] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = definition.getType("int");
        right.explicit = true;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant >> (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant >> (int)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeUSH(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, false);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply unsigned shift [>>>] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = definition.getType("int");
        right.explicit = true;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant >>> (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant >>> (int)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeBWAnd(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, right.actual, false);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply and [&] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant & (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant & (long)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeXor(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteXor(definition, left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply xor [^] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.BOOL) {
                constant = (boolean)left.constant ^ (boolean)right.constant;
            } else if (sort == Sort.INT) {
                constant = (int)left.constant ^ (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant ^ (long)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeBWOr(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = AnalyzerCaster.promoteNumeric(definition, left.actual, right.actual, false);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply or [|] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant | (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant | (long)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        if (actual.sort == Sort.STRING && operation == Operation.ADD) {
            if (!cat) {
                adapter.writeNewStrings();
            }

            left.write(settings, definition, adapter);

            if (!(left instanceof EBinary) || ((EBinary)left).operation != Operation.ADD || left.actual.sort != Sort.STRING) {
                adapter.writeAppendStrings(left.actual);
            }

            right.write(settings, definition, adapter);

            if (!(right instanceof EBinary) || ((EBinary)right).operation != Operation.ADD || right.actual.sort != Sort.STRING) {
                adapter.writeAppendStrings(right.actual);
            }

            if (!cat) {
                adapter.writeToStrings();
            }
        } else {
            left.write(settings, definition, adapter);
            right.write(settings, definition, adapter);

            adapter.writeBinaryInstruction(location, actual, operation);
        }

        adapter.writeBranch(tru, fals);
    }
}
