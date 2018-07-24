/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.capabilities;

import org.elasticsearch.xpack.sql.ServerSqlException;

import java.util.Locale;

import static java.lang.String.format;

/**
 * Thrown when we accidentally attempt to resolve something on on an unresolved entity. Throwing this
 * is always a bug.
 */
public class UnresolvedException extends ServerSqlException {
    public UnresolvedException(String action, Object target) {
        super(format(Locale.ROOT, "Invalid call to %s on an unresolved object %s", action, target));
    }
}
