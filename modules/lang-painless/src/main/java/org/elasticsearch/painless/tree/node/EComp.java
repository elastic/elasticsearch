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
import org.elasticsearch.painless.tree.analyzer.Caster;
import org.elasticsearch.painless.tree.analyzer.Operation;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

public class EComp extends Expression {
    protected final Operation operation;
    protected Expression left;
    protected Expression right;

    public EComp(final String location, final Operation operation, final Expression left, final Expression right) {
        super(location);

        this.operation = operation;
        this.left = left;
        this.right = right;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
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

    protected void analyzeEq(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        if (left.isNull && right.isNull) {
            throw new IllegalArgumentException(error("Extraneous comparison of null constants."));
        }

        final Type promote = Caster.promoteEquality(definition, left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply equals [==] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

        if (left.constant != null && right.constant != null) {
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

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeEqR(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        if (left.isNull && right.isNull) {
            throw new IllegalArgumentException(error("Extraneous comparison of null constants."));
        }

        final Type promote = Caster.promoteReference(definition, left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply reference equals [===] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

        if (left.constant != null && right.constant != null) {
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

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeNE(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        if (left.isNull && right.isNull) {
            throw new IllegalArgumentException(error("Extraneous comparison of null constants."));
        }

        final Type promote = Caster.promoteEquality(definition, left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply not equals [!=] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

        if (left.constant != null && right.constant != null) {
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

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeNER(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        if (left.isNull && right.isNull) {
            throw new IllegalArgumentException(error("Extraneous comparison of null constants."));
        }

        final Type promote = Caster.promoteReference(definition, left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply reference not equals [!==] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

        if (left.constant != null && right.constant != null) {
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

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeGTE(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        if (left.isNull && right.isNull) {
            throw new IllegalArgumentException(error("Extraneous comparison of null constants."));
        }

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply greater than or equals [>=] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

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

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeGT(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        if (left.isNull && right.isNull) {
            throw new IllegalArgumentException(error("Extraneous comparison of null constants."));
        }

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply greater than [>] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

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

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeLTE(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        if (left.isNull && right.isNull) {
            throw new IllegalArgumentException(error("Extraneous comparison of null constants."));
        }

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply less than or equals [<=] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

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

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    protected void analyzeLT(final CompilerSettings settings, final Definition definition, final Variables variables) {
        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        if (left.isNull && right.isNull) {
            throw new IllegalArgumentException(error("Extraneous comparison of null constants."));
        }

        final Type promote = Caster.promoteNumeric(definition, left.actual, right.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply less than [>=] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(definition);
        right = right.cast(definition);

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

        actual = promote;
        typesafe = left.typesafe && right.typesafe;
    }

    @Override
    protected void write(final GeneratorAdapter adapter) {

    }
}
