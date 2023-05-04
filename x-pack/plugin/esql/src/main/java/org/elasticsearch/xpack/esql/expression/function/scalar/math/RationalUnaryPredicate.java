/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

public abstract class RationalUnaryPredicate extends UnaryScalarFunction implements Mappable {
    public RationalUnaryPredicate(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isType(field(), DataType::isRational, sourceText(), null, DataTypes.DOUBLE.typeName());
    }

    @Override
    public final DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public final Object fold() {
        return Mappable.super.fold();
    }
}
