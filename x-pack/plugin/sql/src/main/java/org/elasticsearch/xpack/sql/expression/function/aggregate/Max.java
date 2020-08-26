/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.function.aggregate.EnclosedAgg;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isExact;
import static org.elasticsearch.xpack.sql.expression.SqlTypeResolutions.isNumericOrDateOrTime;

/**
 * Find the maximum value in matching documents.
 */
public class Max extends NumericAggregate implements EnclosedAgg {

    public Max(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Max> info() {
        return NodeInfo.create(this, Max::new, field());
    }

    @Override
    public Max replaceChildren(List<Expression> newChildren) {
        return new Max(source(), newChildren.get(0));
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    public String innerName() {
        return "max";
    }

    @Override
    protected TypeResolution resolveType() {
        if (DataTypes.isString(field().dataType())) {
            return isExact(field(), sourceText(), ParamOrdinal.DEFAULT);
        } else {
            return isNumericOrDateOrTime(field(), sourceText(), ParamOrdinal.DEFAULT);
        }
    }
}
