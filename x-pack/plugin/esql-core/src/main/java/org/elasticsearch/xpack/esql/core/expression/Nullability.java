/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

public enum Nullability {
    /**
     * Whether the expression can become null
     */
    TRUE,

    /**
     * The expression can never become null
     */
    FALSE,

    /**
     * Cannot determine if the expression supports possible null folding
     */
    UNKNOWN
}
