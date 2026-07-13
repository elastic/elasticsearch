/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.capabilities;

import org.elasticsearch.xpack.ql.QlServerException;

/**
 * Thrown when we accidentally attempt to resolve something on on an unresolved entity. Throwing this
 * is always a bug.
 */
public class UnresolvedException extends QlServerException {
    public UnresolvedException(String action, Object target) {
        super("Invalid call to {} on an unresolved object {}", action, target);
    }
}
