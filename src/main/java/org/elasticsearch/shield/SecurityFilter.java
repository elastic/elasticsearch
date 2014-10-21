/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authz.AuthorizationException;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.shield.key.KeyService;
import org.elasticsearch.shield.key.SignatureException;
import org.elasticsearch.shield.transport.ClientTransportFilter;
import org.elasticsearch.shield.transport.ServerTransportFilter;
import org.elasticsearch.transport.TransportRequest;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class SecurityFilter extends AbstractComponent {

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final KeyService keyService;
    private final AuditTrail auditTrail;

    @Inject
    public SecurityFilter(Settings settings, AuthenticationService authcService, AuthorizationService authzService, KeyService keyService, AuditTrail auditTrail) {
        super(settings);
        this.authcService = authcService;
        this.authzService = authzService;
        this.keyService = keyService;
        this.auditTrail = auditTrail;
    }

    User authenticateAndAuthorize(String action, TransportRequest request, User fallbackUser) {
        User user = authcService.authenticate(action, request, fallbackUser);
        authzService.authorize(user, action, request);
        return user;
    }

    User authenticate(RestRequest request) {
        return authcService.authenticate(request);
    }

    <Request extends ActionRequest> Request unsign(User user, String action, Request request) {

        try {

            if (request instanceof SearchScrollRequest) {
                SearchScrollRequest scrollRequest = (SearchScrollRequest) request;
                String scrollId = scrollRequest.scrollId();
                scrollRequest.scrollId(keyService.unsignAndVerify(scrollId));
                return request;
            }

            if (request instanceof ClearScrollRequest) {
                ClearScrollRequest clearScrollRequest = (ClearScrollRequest) request;
                List<String> signedIds = clearScrollRequest.scrollIds();
                List<String> unsignedIds = new ArrayList<>(signedIds.size());
                for (String signedId : signedIds) {
                    unsignedIds.add(keyService.unsignAndVerify(signedId));
                }
                clearScrollRequest.scrollIds(unsignedIds);
                return request;
            }

            return request;

        } catch (SignatureException se) {
            auditTrail.tamperedRequest(user, action, request);
            throw new AuthorizationException("Invalid request: " + se.getMessage());
        }
    }

    <Response extends ActionResponse> Response sign(User user, String action, Response response) {

        if (response instanceof SearchResponse) {
            SearchResponse searchResponse = (SearchResponse) response;
            String scrollId = searchResponse.getScrollId();
            if (scrollId != null && !keyService.signed(scrollId)) {
                searchResponse.scrollId(keyService.sign(scrollId));
            }
            return response;
        }

        return response;
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

            // CORS - allow for preflight unauthenticated OPTIONS request
            if (request.method() != RestRequest.Method.OPTIONS) {
                filter.authenticate(request);
            }

            filterChain.continueProcessing(request, channel);
        }
    }

    public static class ServerTransport implements ServerTransportFilter {

        private final SecurityFilter filter;

        @Inject
        public ServerTransport(SecurityFilter filter) {
            this.filter = filter;
        }

        @Override
        public void inbound(String action, TransportRequest request) {
            // here we don't have a fallback user, as all incoming request are
            // expected to have a user attached (either in headers or in context)
            filter.authenticateAndAuthorize(action, request, null);
        }
    }

    public static class ClientTransport implements ClientTransportFilter {

        private final SecurityFilter filter;

        @Inject
        public ClientTransport(SecurityFilter filter) {
            this.filter = filter;
        }

        @Override
        public void outbound(String action, TransportRequest request) {
            // this will check if there's a user associated with the request. If there isn't,
            // the system user will be attached.
            filter.authcService.attachUserHeaderIfMissing(request, User.SYSTEM);
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
                /**
                 here we fallback on the system user. Internal system requests are requests that are triggered by
                 the system itself (e.g. pings, update mappings, share relocation, etc...) and were not originated
                 by user interaction. Since these requests are triggered by es core modules, they are security
                 agnostic and therefore not associated with any user. When these requests execute locally, they
                 are executed directly on their relevant action. Since there is no other way a request can make
                 it to the action without an associated user (not via REST or transport - this is taken care of by
                 the {@link Rest} filter and the {@link ServerTransport} filter respectively), it's safe to assume a system user
                 here if a request is not associated with any other user.
                */
                User user = filter.authenticateAndAuthorize(action, request, User.SYSTEM);
                request = filter.unsign(user, action, request);
                chain.proceed(action, request, new SigningListener(user, action, filter, listener));
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

    static class SigningListener<Response extends ActionResponse> implements ActionListener<Response> {

        private final User user;
        private final String action;
        private final SecurityFilter filter;
        private final ActionListener innerListener;

        private SigningListener(User user, String action, SecurityFilter filter, ActionListener innerListener) {
            this.user = user;
            this.action = action;
            this.filter = filter;
            this.innerListener = innerListener;
        }

        @Override
        public void onResponse(Response response) {
            response = this.filter.sign(user, action, response);
            innerListener.onResponse(response);
        }

        @Override
        public void onFailure(Throwable e) {
            innerListener.onFailure(e);
        }
    }
}