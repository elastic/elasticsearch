/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

public abstract class Assert {
    // NOCOMMIT we should investigate using java assertions like the rest of the code base

    public static void isTrue(boolean expression, String message, Object... values) {
        if (!expression) {
            throw new SqlIllegalArgumentException(message, values);
        }
    }

    public static void isTrue(boolean expression, String message) {
        if (!expression) {
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
}
