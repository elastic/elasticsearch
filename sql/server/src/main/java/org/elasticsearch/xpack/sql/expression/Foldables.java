/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConvertion;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;

public abstract class Foldables {

    public static <T> T valueOf(Expression e, DataType to) {
        if (e.foldable()) {
            return DataTypeConvertion.convert(e.fold(), e.dataType(), to);
        }
        throw new SqlIllegalArgumentException("Cannot determine value for %s", e);
    }

    public static Object valueOf(Expression e) {
        if (e.foldable()) {
            return e.fold();
        }
        throw new SqlIllegalArgumentException("Cannot determine value for %s", e);
    }

    public static String stringValueOf(Expression e) {
        return valueOf(e, DataTypes.KEYWORD);
    }

    public static Integer intValueOf(Expression e) {
        return valueOf(e, DataTypes.INTEGER);
    }

    public static Long longValueOf(Expression e) {
        return valueOf(e, DataTypes.LONG);
    }

    public static double doubleValueOf(Expression e) {
        return valueOf(e, DataTypes.DOUBLE);
    }

    public static <T> List<T> valuesOf(List<Expression> list, DataType to) {
        List<T> l = new ArrayList<>();
        for (Expression e : list) {
            if (e.foldable()) {
                l.add(DataTypeConvertion.convert(e.fold(), e.dataType(), to));
            }
            else {
                throw new SqlIllegalArgumentException("Cannot determine value for %s", e);
            }
        }
        return l;
    }

    public static List<Double> doubleValuesOf(List<Expression> list) {
        return valuesOf(list, DataTypes.DOUBLE);
    }
}