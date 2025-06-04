/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exception;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * This exception is thrown to indicate that the access has been denied because of role restrictions that
 * an authenticated subject might have (e.g. not allowed to access certain APIs).
 * This differs from other 403 error in sense that it's additional access control that is enforced after role
 * is resolved and before permissions are checked.
 */
public class ElasticsearchRoleRestrictionException extends ElasticsearchSecurityException {

    public ElasticsearchRoleRestrictionException(String msg, Throwable cause, Object... args) {
        super(msg, RestStatus.FORBIDDEN, cause, args);
    }

    public ElasticsearchRoleRestrictionException(String msg, Object... args) {
        this(msg, null, args);
    }

    public ElasticsearchRoleRestrictionException(StreamInput in) throws IOException {
        super(in);
    }
}
