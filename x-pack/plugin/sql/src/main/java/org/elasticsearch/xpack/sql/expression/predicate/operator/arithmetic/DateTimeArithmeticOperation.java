/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.BinaryArithmeticOperation;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

abstract class DateTimeArithmeticOperation extends SqlArithmeticOperation {

    DateTimeArithmeticOperation(Source source, Expression left, Expression right, BinaryArithmeticOperation operation) {
        super(source, left, right, operation);
    }
    
    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        // arithmetic operation can work on:
        // 1. numbers
        // 2. intervals (of compatible types)
        // 3. dates and intervals
        // 4. single unit intervals and numbers

        DataType l = left().dataType();
        DataType r = right().dataType();

        // 1. both are numbers
        if (l.isNumeric() && r.isNumeric()) {
            return TypeResolution.TYPE_RESOLVED;
        }
        // 2. 3. 4. intervals
        if (SqlDataTypes.isInterval(l) || SqlDataTypes.isInterval(r)) {
            if (SqlDataTypeConverter.commonType(l, r) == null) {
                return new TypeResolution(format(null, "[{}] has arguments with incompatible types [{}] and [{}]", symbol(), l, r));
            } else {
                return resolveWithIntervals();
            }
        }

        // fall-back to default checks
        return super.resolveType();
    }

    protected TypeResolution resolveWithIntervals() {
        DataType l = left().dataType();
        DataType r = right().dataType();

        if ((SqlDataTypes.isDateOrTimeBased(r) || SqlDataTypes.isInterval(r) || DataTypes.isNull(r)) == false
                || (SqlDataTypes.isDateOrTimeBased(l) || SqlDataTypes.isInterval(l) || DataTypes.isNull(l)) == false) {
            return new TypeResolution(format(null, "[{}] has arguments with incompatible types [{}] and [{}]", symbol(), l, r));
        }
        return TypeResolution.TYPE_RESOLVED;
    }
}
