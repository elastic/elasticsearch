/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

abstract class DateTimeArithmeticOperation extends ArithmeticOperation {

    DateTimeArithmeticOperation(Source source, Expression left, Expression right, BinaryArithmeticOperation operation) {
        super(source, left, right, operation);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        // arithmetic operation can work on numbers in QL

        DataType l = left().dataType();
        DataType r = right().dataType();

        // 1. both are numbers
        if (l.isNumeric() && r.isNumeric()) {
            return TypeResolution.TYPE_RESOLVED;
        }

        // fall-back to default checks
        return super.resolveType();
    }
}
