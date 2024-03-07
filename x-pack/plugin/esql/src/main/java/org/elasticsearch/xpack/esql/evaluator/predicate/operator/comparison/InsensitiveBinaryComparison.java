/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.qlcore.tree.Source;
import org.elasticsearch.xpack.qlcore.type.DataType;
import org.elasticsearch.xpack.qlcore.type.DataTypes;

public abstract class InsensitiveBinaryComparison extends BinaryScalarFunction {

    protected InsensitiveBinaryComparison(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

}
