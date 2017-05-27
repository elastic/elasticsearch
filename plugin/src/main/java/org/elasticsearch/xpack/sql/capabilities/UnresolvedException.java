/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.capabilities;

import java.util.Locale;

import org.elasticsearch.xpack.sql.SqlException;

import static java.lang.String.format;

public class UnresolvedException extends SqlException {

    public UnresolvedException(String action, Object target) {
        super(format(Locale.ROOT, "Invalid call to %s on an unresolved object %s", action, target));
    }
}
