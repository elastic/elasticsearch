/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public abstract class Foldables {

    @SuppressWarnings("unchecked")
    private static <T> T valueOf(Expression e, DataType to) {
        if (e.foldable()) {
            return (T) SqlDataTypeConverter.convert(e.fold(), to);
        }
        throw new QlIllegalArgumentException("Cannot determine value for {}", e);
    }

    public static Object valueOf(Expression e) {
        if (e.foldable()) {
            return e.fold();
        }
        throw new QlIllegalArgumentException("Cannot determine value for {}", e);
    }

    public static Integer intValueOf(Expression e) {
        return valueOf(e, DataTypes.INTEGER);
    }

    public static double doubleValueOf(Expression e) {
        return valueOf(e, DataTypes.DOUBLE);
    }

    public static <T> List<T> valuesOf(List<Expression> list, DataType to) {
        return foldTo(list, to, new ArrayList<>(list.size()));
    }

    public static <T> Set<T> valuesUnique(List<Expression> list, DataType to) {
        return foldTo(list, to, new LinkedHashSet<>(list.size()));
    }

    private static <T, C extends Collection<T>> C foldTo(Collection<Expression> expressions, DataType to, C values) {
        for (Expression e : expressions) {
            values.add(valueOf(e, to));
        }
        return values;
    }

    public static List<Double> doubleValuesOf(List<Expression> list) {
        return valuesOf(list, DataTypes.DOUBLE);
    }
}
