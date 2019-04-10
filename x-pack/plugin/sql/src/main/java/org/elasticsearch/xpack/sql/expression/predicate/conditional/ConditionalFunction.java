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
import org.elasticsearch.xpack.sql.type.DataTypeConversion;

import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.sql.type.DataTypes.areTypesCompatible;
import static org.elasticsearch.xpack.sql.util.StringUtils.ordinal;

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
    protected TypeResolution resolveType() {
        DataType dt = DataType.NULL;

        for (int i = 0; i < children().size(); i++) {
            Expression child = children().get(i);
            if (dt == DataType.NULL) {
                if (Expressions.isNull(child) == false) {
                    dt = child.dataType();
                }
            } else {
                if (areTypesCompatible(dt, child.dataType()) == false) {
                    return new TypeResolution(format(null, "{} argument of [{}] must be [{}], found value [{}] type [{}]",
                        ordinal(i + 1),
                        sourceText(),
                        dt.typeName,
                        Expressions.name(child),
                        child.dataType().typeName));
                }
            }
            dataType = DataTypeConversion.commonType(dataType, child.dataType());
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Nullability nullable() {
        return Nullability.UNKNOWN;
    }
}
