/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
//Binary expression that requires both input expressions to have the same type
//Compatible types should be handled by the analyzer (by using the narrowest type)
public abstract class BinaryOperator extends BinaryExpression {

    public interface Negateable {
        BinaryExpression negate();
    }

    protected BinaryOperator(Location location, Expression left, Expression right) {
        super(location, left, right);
    }

    protected abstract TypeResolution resolveInputType(DataType inputType);

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
