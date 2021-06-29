/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.util.StringUtils.ordinal;

/**
 * Base class for conditional predicates.
 */
public abstract class ConditionalFunction extends ScalarFunction {

    protected DataType dataType = null;

    ConditionalFunction(Source source, List<Expression> fields) {
        super(source, fields);
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            dataType = DataTypes.NULL;
            for (Expression exp : children()) {
                dataType = SqlDataTypeConverter.commonType(dataType, exp.dataType());
            }
        }
        return dataType;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    protected TypeResolution resolveType() {
        DataType dt = DataTypes.NULL;

        for (int i = 0; i < children().size(); i++) {
            Expression child = children().get(i);
            if (dt == DataTypes.NULL) {
                if (Expressions.isNull(child) == false) {
                    dt = child.dataType();
                }
            } else {
                if (SqlDataTypes.areCompatible(dt, child.dataType()) == false) {
                    return new TypeResolution(format(null, "{} argument of [{}] must be [{}], found value [{}] type [{}]",
                        ordinal(i + 1),
                        sourceText(),
                        dt.typeName(),
                        Expressions.name(child),
                        child.dataType().typeName()));
                }
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Nullability nullable() {
        return Nullability.UNKNOWN;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConditionalFunction that = (ConditionalFunction) o;
        return children().equals(that.children());
    }

    @Override
    public int hashCode() {
        return Objects.hash(children());
    }
}
