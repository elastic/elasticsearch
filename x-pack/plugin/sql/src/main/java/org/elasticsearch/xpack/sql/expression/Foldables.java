/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;

import java.util.ArrayList;
import java.util.List;

public abstract class Foldables {

    @SuppressWarnings("unchecked")
    public static <T> T valueOf(Expression e, DataType to) {
        if (e.foldable()) {
            return (T) DataTypeConversion.conversionFor(e.dataType(), to).convert(e.fold());
        }
        throw new SqlIllegalArgumentException("Cannot determine value for {}", e);
    }

    public static Object valueOf(Expression e) {
        if (e.foldable()) {
            return e.fold();
        }
        throw new SqlIllegalArgumentException("Cannot determine value for {}", e);
    }

    public static String stringValueOf(Expression e) {
        return valueOf(e, DataType.KEYWORD);
    }

    public static Integer intValueOf(Expression e) {
        return valueOf(e, DataType.INTEGER);
    }

    public static Long longValueOf(Expression e) {
        return valueOf(e, DataType.LONG);
    }

    public static double doubleValueOf(Expression e) {
        return valueOf(e, DataType.DOUBLE);
    }

    public static <T> List<T> valuesOf(List<Expression> list, DataType to) {
        List<T> l = new ArrayList<>(list.size());
        for (Expression e : list) {
            l.add(valueOf(e, to));
        }
        return l;
    }

    public static List<Double> doubleValuesOf(List<Expression> list) {
        return valuesOf(list, DataType.DOUBLE);
    }
}
