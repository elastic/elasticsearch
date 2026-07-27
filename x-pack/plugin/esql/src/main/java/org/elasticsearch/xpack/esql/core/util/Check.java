/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

/**
 * Utility class used for checking various conditions at runtime, with minimum amount of code.
 *
 * <p>Two families, and the choice between them decides the HTTP status the caller sees:
 * <ul>
 *   <li>{@link #clientError} rejects <b>caller-supplied input</b> — a bad setting value, an unusable request.
 *       It throws {@link IllegalArgumentException}, which {@code ExceptionsHelper#status} maps to 400.</li>
 *   <li>{@link #isTrue}, {@link #notNull} and friends guard <b>internal invariants</b> — states that are
 *       unreachable unless there is a bug. They throw {@link QlIllegalArgumentException}, which despite its name
 *       extends {@code QlServerException} and maps to 500.</li>
 * </ul>
 * Reaching for {@code isTrue} to validate user input reports that user's mistake as a server fault, which is how
 * a family of external-data-source settings came to answer 500 for values the registration API rejected as 400.
 */
public abstract class Check {

    /**
     * Fails with a <b>client</b> error (400) when {@code expression} is false. Use whenever the condition can be
     * false because of something a user or operator supplied; use {@link #isTrue}, which fails with a server
     * error (500), for conditions only a bug can violate.
     */
    public static void clientError(boolean expression, String message, Object... values) {
        if (expression == false) {
            throw new IllegalArgumentException(LoggerMessageFormat.format(message, values));
        }
    }

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
