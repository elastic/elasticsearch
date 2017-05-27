/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConvertion;

public abstract class BinaryOperator extends BinaryExpression {

    protected BinaryOperator(Location location, Expression left, Expression right) {
        super(location, left, right);
    }

    protected abstract DataType acceptedType();

    @Override
    protected TypeResolution resolveType() {
        DataType accepted = acceptedType();
        DataType l = left().dataType();
        DataType r = right().dataType();
        
        if (!l.same(r)) {
            return new TypeResolution("Different types (%s and %s) used in '%s'", l.sqlName(), r.sqlName(), symbol());
        }
        if (!DataTypeConvertion.canConvert(accepted, left().dataType())) {
            return new TypeResolution("'%s' requires type %s not %s", symbol(), accepted.sqlName(), l.sqlName());
        }
        else {
            return TypeResolution.TYPE_RESOLVED;
        }
    }
}
