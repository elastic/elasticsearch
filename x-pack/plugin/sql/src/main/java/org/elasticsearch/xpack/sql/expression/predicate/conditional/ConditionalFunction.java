/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.List;

/**
 * Base class for conditional predicates.
 */
public abstract class ConditionalFunction extends ScalarFunction {

    protected DataType dataType = DataType.NULL;

    ConditionalFunction(Source source, List<Expression> fields) {
        super(source, fields);
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public Nullability nullable() {
        return Nullability.UNKNOWN;
    }
}
