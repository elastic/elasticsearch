/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.core.Assertions;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.function.Supplier;

public class ExceptionUtils {
    /**
     * Create a {@link VerificationException} from an {@link ArithmeticException} thrown because of an invalid math expression.
     *
     * @param source the invalid part of the query causing the exception
     * @param e the exception that was thrown
     * @return an exception with a user-readable error message with http code 400
     */
    public static VerificationException math(Source source, ArithmeticException e) {
        return new VerificationException("arithmetic exception in expression [{}]: [{}]", source.text(), e.getMessage());
    }

    /**
     * We generally prefer to avoid assertions in production code, as they can kill the entire node if they fail, instead of just failing
     * a given test. So instead, we throw an {@link IllegalStateException}. Like proper asserts, this will only be executed if assertions
     * are enabled.
     */
    public static void assertIllegalState(Boolean condition, String message) {
        if (Assertions.ENABLED && condition == false) {
            throw new IllegalStateException(message);
        }
    }

    /**
     * We generally prefer to avoid assertions in production code, as they can kill the entire node if they fail, instead of just failing
     * a given test. So instead, we throw an {@link IllegalStateException}. Like proper asserts, this will only be executed if assertions
     * are enabled.
     */
    public static void assertIllegalState(Supplier<Boolean> condition, String message) {
        if (Assertions.ENABLED && condition.get() == false) {
            throw new IllegalStateException(message);
        }
    }
}
