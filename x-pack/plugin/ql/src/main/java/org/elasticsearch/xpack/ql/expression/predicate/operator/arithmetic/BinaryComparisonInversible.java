/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;

/*
 * Factory interface for arithmetic operations that have an inverse in reference to a binary comparison.
 * For instance the division is multiplication's inverse, substitution addition's, log exponentiation's a.s.o.
 * Not all operations - like modulo - are invertible.
 */
public interface BinaryComparisonInversible {

    interface ArithmeticOperationFactory {
        ArithmeticOperation create(Source source, Expression left, Expression right);
    }

    ArithmeticOperationFactory binaryComparisonInverse();
}
