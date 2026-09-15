/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestContentTypePolicy;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;

import java.io.IOException;

import static org.elasticsearch.core.Strings.format;

public class SecurityRestContentTypePolicy implements RestContentTypePolicy {

    private static final Logger logger = LogManager.getLogger(SecurityRestContentTypePolicy.class);

    private final boolean enabled;
    private final boolean httpSslEnabled;
    private final ThreadContext threadContext;
    private final AuthenticationContextSerializer authenticationSerializer = new AuthenticationContextSerializer();

    public SecurityRestContentTypePolicy(boolean enabled, boolean httpSslEnabled, ThreadContext threadContext) {
        this.enabled = enabled;
        this.httpSslEnabled = httpSslEnabled;
        this.threadContext = threadContext;
    }

    @Override
    public boolean allowsBrowserSafelistedContentType(RestRequest request) {
        if (enabled == false || httpSslEnabled == false) {
            return false;
        }
        try {
            final Authentication authentication = authenticationSerializer.readFromContext(threadContext);
            return authentication != null && authentication.getAuthenticationType() != Authentication.AuthenticationType.ANONYMOUS;
        } catch (IOException e) {
            logger.debug(() -> format("failed to read authentication for REST request [%s]", request.uri()), e);
            return false;
        }
    }
}
