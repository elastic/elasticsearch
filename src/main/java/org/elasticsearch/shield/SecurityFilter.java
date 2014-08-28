/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authc.system.SystemRealm;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.shield.transport.TransportFilter;
import org.elasticsearch.transport.TransportRequest;

/**
 *
 */
public class SecurityFilter extends AbstractComponent {

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;

    @Inject
    public SecurityFilter(Settings settings, AuthenticationService authcService, AuthorizationService authzService, RestController restController) {
        super(settings);
        this.authcService = authcService;
        this.authzService = authzService;
        restController.registerFilter(new Rest(this));
    }

    void process(String action, TransportRequest request) {
        AuthenticationToken token = authcService.token(action, request, SystemRealm.TOKEN);
        User user = authcService.authenticate(action, request, token);
        authzService.authorize(user, action, request);
    }

    public static class Rest extends RestFilter {

        private final SecurityFilter filter;

        public Rest(SecurityFilter filter) {
            this.filter = filter;
        }

        @Override
        public int order() {
            return Integer.MIN_VALUE;
        }

        @Override
        public void process(RestRequest request, RestChannel channel, RestFilterChain filterChain) throws Exception {
            filter.authcService.verifyToken(request);
            filterChain.continueProcessing(request, channel);
        }
    }

    public static class Transport extends TransportFilter.Base {

        private final SecurityFilter filter;

        @Inject
        public Transport(SecurityFilter filter) {
            this.filter = filter;
        }

        @Override
        public void inboundRequest(String action, TransportRequest request) {
            filter.process(action, request);
        }
    }

    public static class Action implements org.elasticsearch.action.support.ActionFilter {

        private final SecurityFilter filter;

        @Inject
        public Action(SecurityFilter filter) {
            this.filter = filter;
        }

        @Override
        public void process(String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
            try {
                filter.process(action, request);
            } catch (Throwable t) {
                listener.onFailure(t);
                return;
            }
            chain.continueProcessing(action, request, listener);
        }

        @Override
        public int order() {
            return Integer.MIN_VALUE;
        }
    }
}