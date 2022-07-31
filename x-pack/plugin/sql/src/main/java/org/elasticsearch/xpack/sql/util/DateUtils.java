/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.util;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

public final class DateUtils {

    private static final int DEFAULT_PRECISION_FOR_CURRENT_FUNCTIONS = 3;

    private DateUtils() {}


    public static int getNanoPrecision(Expression precisionExpression, int nano) {
        int precision = DEFAULT_PRECISION_FOR_CURRENT_FUNCTIONS;

        if (precisionExpression != null) {
            try {
                precision = (Integer) SqlDataTypeConverter.convert(Foldables.valueOf(precisionExpression), DataTypes.INTEGER);
            } catch (Exception e) {
                throw new ParsingException(precisionExpression.source(), "invalid precision; " + e.getMessage());
            }
        }

        if (precision < 0 || precision > 9) {
            throw new ParsingException(
                precisionExpression.source(),
                "precision needs to be between [0-9], received [{}]",
                precisionExpression.sourceText()
            );
        }

        // remove the remainder
        nano = nano - nano % (int) Math.pow(10, (9 - precision));
        return nano;
    }
}
