/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

/**
 * Exception thrown when a :: selector is not supported.
 */
public class UnsupportedSelectorException extends IllegalArgumentException {

    public UnsupportedSelectorException(String expression) {
        super("Index component selectors are not supported in this context but found selector in expression [" + expression + "]");
    }

}
