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

package org.elasticsearch.painless.tree.node;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Utility;
import org.elasticsearch.painless.tree.analyzer.Caster;
import org.elasticsearch.painless.tree.analyzer.Operation;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

public class EBinary extends Expression {
    protected final Operation operation;
    protected Expression left;
    protected Expression right;

    public EBinary(final String location, final Operation operation, final Expression left, final Expression right) {
        super(location);

        this.operation = operation;
        this.left = left;
        this.right = right;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
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

    protected void analyzeMul(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply multiply [*] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

        if (left.constant != null && right.constant != null) {
            final boolean overflow = settings.getNumericOverflow();
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = overflow ? (int)left.constant * (int)right.constant :
                    Math.multiplyExact((int)left.constant, (int)right.constant);
            } else if (sort == Sort.LONG) {
                constant = overflow ? (long)left.constant * (long)right.constant :
                    Math.multiplyExact((long)left.constant, (long)right.constant);
            } else if (sort == Sort.FLOAT) {
                constant = overflow ? (float)left.constant * (float)right.constant :
                    Utility.multiplyWithoutOverflow((float)left.constant, (float)right.constant);
            } else if (sort == Sort.DOUBLE) {
                constant = overflow ? (double)left.constant * (double)right.constant :
                    Utility.multiplyWithoutOverflow((double)left.constant, (double)right.constant);
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeDiv(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply divide [/] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

        if (left.constant != null && right.constant != null) {
            final boolean overflow = settings.getNumericOverflow();
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = overflow ? (int)left.constant / (int)right.constant :
                    Utility.divideWithoutOverflow((int)left.constant, (int)right.constant);
            } else if (sort == Sort.LONG) {
                constant = overflow ? (long)left.constant / (long)right.constant :
                    Utility.divideWithoutOverflow((long)left.constant, (long)right.constant);
            } else if (sort == Sort.FLOAT) {
                constant = overflow ? (float)left.constant / (float)right.constant :
                    Utility.divideWithoutOverflow((float)left.constant, (float)right.constant);
            } else if (sort == Sort.DOUBLE) {
                constant = overflow ? (double)left.constant / (double)right.constant :
                    Utility.divideWithoutOverflow((double)left.constant, (double)right.constant);
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeRem(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply remainder [%] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

        if (left.constant != null && right.constant != null) {
            final boolean overflow = settings.getNumericOverflow();
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant % (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant % (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = overflow ? (float)left.constant % (float)right.constant :
                    Utility.remainderWithoutOverflow((float)left.constant, (float)right.constant);
            } else if (sort == Sort.DOUBLE) {
                constant = overflow ? (double)left.constant % (double)right.constant :
                    Utility.remainderWithoutOverflow((double)left.constant, (double)right.constant);
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeAdd(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = Caster.promoteAdd(definition, left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply add [+] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        final Sort sort = promote.sort;

        if (sort == Sort.STRING) {
            left.expected = left.actual;
            left.strings = true;
            right.expected = right.actual;
            right.strings = true;
        } else {
            left.expected = promote;
            right.expected = promote;
        }

        left = left.cast(definition);
        right = right.cast(definition);

        if (left.constant != null && right.constant != null) {
            final boolean overflow = settings.getNumericOverflow();

            if (sort == Sort.INT) {
                constant = overflow ? (int)left.constant + (int)right.constant :
                    Math.addExact((int)left.constant, (int)right.constant);
            } else if (sort == Sort.LONG) {
                constant = overflow ? (long)left.constant + (long)right.constant :
                    Math.addExact((long)left.constant, (long)right.constant);
            } else if (sort == Sort.FLOAT) {
                constant = overflow ? (float)left.constant + (float)right.constant :
                    Utility.addWithoutOverflow((float)left.constant, (float)right.constant);
            } else if (sort == Sort.DOUBLE) {
                constant = overflow ? (double)left.constant + (double)right.constant :
                    Utility.addWithoutOverflow((double)left.constant, (double)right.constant);
            } else if (sort == Sort.STRING) {
                constant = "" + left.constant + right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeSub(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply subtract [-] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

        if (left.constant != null && right.constant != null) {
            final boolean overflow = settings.getNumericOverflow();
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = overflow ? (int)left.constant - (int)right.constant :
                    Math.subtractExact((int)left.constant, (int)right.constant);
            } else if (sort == Sort.LONG) {
                constant = overflow ? (long)left.constant - (long)right.constant :
                    Math.subtractExact((long)left.constant, (long)right.constant);
            } else if (sort == Sort.FLOAT) {
                constant = overflow ? (float)left.constant - (float)right.constant :
                    Utility.subtractWithoutOverflow((float)left.constant, (float)right.constant);
            } else if (sort == Sort.DOUBLE) {
                constant = overflow ? (double)left.constant - (double)right.constant :
                    Utility.subtractWithoutOverflow((double)left.constant, (double)right.constant);
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeLSH(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, false, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply left shift [<<] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.typesafe = false;
        right.expected = definition.intType;

        left = left.cast(definition);
        right = right.cast(definition);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant << (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant << (long)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeRSH(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, false, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply right shift [>>] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.typesafe = false;
        right.expected = definition.intType;

        left = left.cast(definition);
        right = right.cast(definition);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant >> (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant >> (long)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeUSH(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, false, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply unsigned shift [>>>] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.typesafe = false;
        right.expected = definition.intType;

        left = left.cast(definition);
        right = right.cast(definition);

        if (left.constant != null && right.constant != null) {
            final Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant >>> (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant >>> (long)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeBWAnd(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, false, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply and [&] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

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
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeXor(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = Caster.promoteXor(definition, left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply xor [^] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

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
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeBWOr(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, false, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply or [|] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

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
        typesafe = left.typesafe && right.typesafe;
    }

    @Override
    protected void write(final GeneratorAdapter adapter) {

    }
}
