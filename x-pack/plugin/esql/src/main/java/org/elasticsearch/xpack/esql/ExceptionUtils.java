/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.esql.core.tree.Source;

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
}
