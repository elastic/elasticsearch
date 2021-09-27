/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.rest.RestStatus;

public class ElasticsearchAuthenticationProcessException extends ElasticsearchSecurityException {

    public ElasticsearchAuthenticationProcessException(String msg, RestStatus status, Throwable cause, Object... args) {
        super(msg, status, cause, args);
    }
}
