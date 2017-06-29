/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.util;

import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcException;
import org.elasticsearch.xpack.sql.net.client.util.StringUtils;

public class Assert {

    public static void hasText(CharSequence sequence, String message) {
        if (!StringUtils.hasText(sequence)) {
            throw new JdbcException(message);
        }
    }

    public static void hasText(CharSequence sequence, String message, Object... values) {
        if (!StringUtils.hasText(sequence)) {
            throw new JdbcException(message, values);
        }
    }

    public static void isTrue(boolean expression, String message, Object... values) {
        if (!expression) {
            throw new JdbcException(message, values);
        }
    }

    public static void isTrue(boolean expression, String message) {
        if (!expression) {
            throw new JdbcException(message);
        }
    }

    public static void notNull(Object object, String message) {
        if (object == null) {
            throw new JdbcException(message);
        }
    }

    public static void notNull(Object object, String message, Object... values) {
        if (object == null) {
            throw new JdbcException(message, values);
        }
    }
}
