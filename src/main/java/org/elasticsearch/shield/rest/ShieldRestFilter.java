/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.rest;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.*;
import org.elasticsearch.shield.authc.AuthenticationService;

/**
 *
 */
public class ShieldRestFilter extends RestFilter {

    private final AuthenticationService service;

    @Inject
    public ShieldRestFilter(AuthenticationService service, RestController controller) {
        this.service = service;
        controller.registerFilter(this);
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    @Override
    public void process(RestRequest request, RestChannel channel, RestFilterChain filterChain) throws Exception {

        // CORS - allow for preflight unauthenticated OPTIONS request
        if (request.method() != RestRequest.Method.OPTIONS) {
            service.authenticate(request);
        }

        RemoteHostHeader.process(request);

        filterChain.continueProcessing(request, channel);
    }
}
