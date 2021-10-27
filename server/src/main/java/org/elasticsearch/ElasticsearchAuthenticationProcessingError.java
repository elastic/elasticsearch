/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Used to indicate that the authentication process encountered a server-side error (5xx) that prevented the credentials verification.
 * The presented client credentials might or might not be valid.
 * This differs from an authentication failure error in subtle ways. This should be preferred when the issue hindering credentials
 * verification is transient, such as network congestion or overloaded instances, but not in cases of misconfiguration.
 * However this distinction is further blurred because in certain configurations and for certain credential types, the same
 * credential can be validated in multiple ways, only some of which might experience transient problems.
 * When in doubt, rely on the implicit behavior of 401 authentication failure.
 */
public class ElasticsearchAuthenticationProcessingError extends ElasticsearchSecurityException {

    public ElasticsearchAuthenticationProcessingError(String msg, RestStatus status, Throwable cause, Object... args) {
        super(msg, status, cause, args);
        assert status == RestStatus.INTERNAL_SERVER_ERROR || status == RestStatus.SERVICE_UNAVAILABLE;
    }

    public ElasticsearchAuthenticationProcessingError(StreamInput in) throws IOException {
        super(in);
    }
}
