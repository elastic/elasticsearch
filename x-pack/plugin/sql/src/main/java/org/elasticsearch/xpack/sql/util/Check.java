/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.util;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

/**
 * Utility class used for checking various conditions at runtime, inside SQL (hence the specific exception) with
 * minimum amount of code
 */
public abstract class Check {

    public static void isTrue(boolean expression, String message, Object... values) {
        if (expression == false) {
            throw new SqlIllegalArgumentException(message, values);
        }
    }

    public static void isTrue(boolean expression, String message) {
        if (expression == false) {
            throw new SqlIllegalArgumentException(message);
        }
    }

    public static void notNull(Object object, String message) {
        if (object == null) {
            throw new SqlIllegalArgumentException(message);
        }
    }

    public static void notNull(Object object, String message, Object... values) {
        if (object == null) {
            throw new SqlIllegalArgumentException(message, values);
        }
    }

    public static void isFixedNumberAndInRange(Object object, String objectName, Long from, Long to) {
        if ((object instanceof Number) == false || object instanceof Float || object instanceof Double) {
            throw new SqlIllegalArgumentException(
                "A fixed point number is required for [{}]; received [{}]",
                objectName,
                object.getClass().getTypeName()
            );
        }
        Long longValue = ((Number) object).longValue();
        if (longValue < from || longValue > to) {
            throw new SqlIllegalArgumentException("[{}] out of the allowed range [{}, {}], received [{}]", objectName, from, to, longValue);
        }
    }
}
