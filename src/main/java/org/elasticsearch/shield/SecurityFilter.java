/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authc.system.SystemRealm;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.shield.authz.SystemRole;
import org.elasticsearch.shield.transport.TransportFilter;
import org.elasticsearch.transport.TransportRequest;

/**
 *
 */
public class SecurityFilter extends AbstractComponent {

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;

    @Inject
    public SecurityFilter(Settings settings, AuthenticationService authcService, AuthorizationService authzService) {
        super(settings);
        this.authcService = authcService;
        this.authzService = authzService;
    }

    void process(String action, TransportRequest request) {

        // if the action is a system action, we'll fall back on the system user, otherwise we
        // won't fallback on any user and an authentication exception will be thrown
        AuthenticationToken defaultToken = SystemRole.INSTANCE.check(action) ? SystemRealm.TOKEN : null;

        AuthenticationToken token = authcService.token(action, request, defaultToken);
        User user = authcService.authenticate(action, request, token);
        authzService.authorize(user, action, request);
    }

    public static class Rest extends RestFilter {

        private final SecurityFilter filter;

        @Inject
        public Rest(SecurityFilter filter, RestController controller) {
            this.filter = filter;
            controller.registerFilter(this);
        }

        @Override
        public int order() {
            return Integer.MIN_VALUE;
        }

        @Override
        public void process(RestRequest request, RestChannel channel, RestFilterChain filterChain) throws Exception {
            filter.authcService.extractAndRegisterToken(request);
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

    public static class Action implements ActionFilter {

        private final SecurityFilter filter;

        @Inject
        public Action(SecurityFilter filter) {
            this.filter = filter;
        }

        @Override
        public void apply(String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
            try {
                filter.process(action, request);
                chain.proceed(action, request, listener);
            } catch (Throwable t) {
                listener.onFailure(t);
            }
        }

        @Override
        public void apply(String action, ActionResponse response, ActionListener listener, ActionFilterChain chain) {
            chain.proceed(action, response, listener);
        }

        @Override
        public int order() {
            return Integer.MIN_VALUE;
        }
    }
}