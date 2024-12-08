/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

/**
 * Utility class used for checking various conditions at runtime, with minimum amount of code.
 */
public abstract class Check {

    public static void isTrue(boolean expression, String message, Object... values) {
        if (expression == false) {
            throw new QlIllegalArgumentException(message, values);
        }
    }

    public static void isTrue(boolean expression, String message) {
        if (expression == false) {
            throw new QlIllegalArgumentException(message);
        }
    }

    public static void notNull(Object object, String message) {
        if (object == null) {
            throw new QlIllegalArgumentException(message);
        }
    }

    public static void notNull(Object object, String message, Object... values) {
        if (object == null) {
            throw new QlIllegalArgumentException(message, values);
        }
    }

    public static void isString(Object obj) {
        if ((obj instanceof String || obj instanceof Character) == false) {
            throw new QlIllegalArgumentException("A string/char is required; received [{}]", obj);
        }
    }

    public static void isBoolean(Object obj) {
        if ((obj instanceof Boolean) == false) {
            throw new QlIllegalArgumentException("A boolean is required; received [{}]", obj);
        }
    }
}
