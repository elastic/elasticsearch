/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * Operator is a specialized binary predicate where both sides have the compatible types
 * (it's up to the analyzer to do any conversion if needed).
 */
public abstract class BinaryOperator extends BinaryPredicate {

    public interface Negateable {
        BinaryOperator negate();
    }

    protected BinaryOperator(Location location, Expression left, Expression right, String symbol) {
        super(location, left, right, symbol);
    }

    protected abstract TypeResolution resolveInputType(DataType inputType);

    public abstract BinaryOperator swapLeftAndRight();

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }
        DataType l = left().dataType();
        DataType r = right().dataType();

        TypeResolution resolution = resolveInputType(l);

        if (resolution == TypeResolution.TYPE_RESOLVED) {
            return resolveInputType(r);
        }
        return resolution;
    }
}