/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicenseExpiredException;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.authz.AuthorizationException;
import org.elasticsearch.shield.authz.AuthorizationService;
import org.elasticsearch.shield.authz.Privilege;
import org.elasticsearch.shield.license.LicenseEventsNotifier;
import org.elasticsearch.shield.license.LicenseService;
import org.elasticsearch.shield.signature.SignatureException;
import org.elasticsearch.shield.signature.SignatureService;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ShieldActionFilter extends AbstractComponent implements ActionFilter {

    public static final String CLUSTER_PERMISSION_SCROLL_CLEAR_ALL_NAME = "cluster:admin/indices/scroll/clear_all";

    private static final Predicate<String> READ_ACTION_MATCHER = Privilege.Index.READ.predicate();

    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final SignatureService signatureService;
    private final AuditTrail auditTrail;

    private volatile boolean licenseEnabled;

    @Inject
    public ShieldActionFilter(Settings settings, AuthenticationService authcService, AuthorizationService authzService, SignatureService signatureService, AuditTrail auditTrail, LicenseEventsNotifier licenseEventsNotifier) {
        super(settings);
        this.authcService = authcService;
        this.authzService = authzService;
        this.signatureService = signatureService;
        this.auditTrail = auditTrail;
        licenseEventsNotifier.register(new LicensesClientService.Listener() {
            @Override
            public void onEnabled() {
                licenseEnabled = true;
            }

            @Override
            public void onDisabled() {
                licenseEnabled = false;
            }
        });
    }

    @Override
    public void apply(String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {

        /**
            A functional requirement - when the license of shield is disabled (invalid/expires), shield will continue
            to operate normally, except all read operations will be blocked.
         */
        if (!licenseEnabled && READ_ACTION_MATCHER.apply(action)) {
            logger.error("Blocking read operation [" + action + "] due to disabled license");
            throw new LicenseExpiredException(LicenseService.FEATURE_NAME);
        }

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
            User user = authcService.authenticate(action, request, User.SYSTEM);
            authzService.authorize(user, action, request);
            request = unsign(user, action, request);
            chain.proceed(action, request, new SigningListener(this, listener));
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

    <Request extends ActionRequest> Request unsign(User user, String action, Request request) {

        try {

            if (request instanceof SearchScrollRequest) {
                SearchScrollRequest scrollRequest = (SearchScrollRequest) request;
                String scrollId = scrollRequest.scrollId();
                scrollRequest.scrollId(signatureService.unsignAndVerify(scrollId));
                return request;
            }

            if (request instanceof ClearScrollRequest) {
                ClearScrollRequest clearScrollRequest = (ClearScrollRequest) request;
                boolean isClearAllScrollRequest = clearScrollRequest.scrollIds().contains("_all");
                if (isClearAllScrollRequest) {
                    authzService.authorize(user, CLUSTER_PERMISSION_SCROLL_CLEAR_ALL_NAME, request);
                } else {
                    List<String> signedIds = clearScrollRequest.scrollIds();
                    List<String> unsignedIds = new ArrayList<>(signedIds.size());
                    for (String signedId : signedIds) {
                        unsignedIds.add(signatureService.unsignAndVerify(signedId));
                    }
                    clearScrollRequest.scrollIds(unsignedIds);
                }
                return request;
            }

            return request;

        } catch (SignatureException se) {
            auditTrail.tamperedRequest(user, action, request);
            throw new AuthorizationException("Invalid request: " + se.getMessage());
        }
    }

    <Response extends ActionResponse> Response sign(Response response) {

        if (response instanceof SearchResponse) {
            SearchResponse searchResponse = (SearchResponse) response;
            String scrollId = searchResponse.getScrollId();
            if (scrollId != null && !signatureService.signed(scrollId)) {
                searchResponse.scrollId(signatureService.sign(scrollId));
            }
            return response;
        }

        return response;
    }

    static class SigningListener<Response extends ActionResponse> implements ActionListener<Response> {

        private final ShieldActionFilter filter;
        private final ActionListener innerListener;

        private SigningListener(ShieldActionFilter filter, ActionListener innerListener) {
            this.filter = filter;
            this.innerListener = innerListener;
        }

        @Override @SuppressWarnings("unchecked")
        public void onResponse(Response response) {
            response = this.filter.sign(response);
            innerListener.onResponse(response);
        }

        @Override
        public void onFailure(Throwable e) {
            innerListener.onFailure(e);
        }
    }
}
